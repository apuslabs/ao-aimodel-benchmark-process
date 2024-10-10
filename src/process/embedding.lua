Json = require("json")
Log = require("module.utils.log")
local Helper = require("module.utils.helper")
Config = require("module.utils.config")
local Datetime = require("module.utils.datetime")
local Lodash = require("module.utils.lodash")
ObjectUtils = require("module.utils.object")
ArrayUtils = require("module.utils.array")

local throttleCheck = Helper.throttleCheckWrapper(Config.Pool.JoinThrottle)

UploadedUserList = UploadedUserList or {}
UploadedDatasetList = UploadedDatasetList or {}
function RemoveUserFromUploadedList(address)
    if UploadedUserList[address] then
        UploadedUserList[address] = nil
        Log.warn(string.format("Removed %s from uploaded list", address))
    end
end

WhiteList = WhiteList or {}
Handlers.add("Check-Permission", "Check-Permission", function (msg)
    local From = msg.FromAddress or msg.From
    msg.reply({ Status = "200", Data = Lodash.Contain(WhiteList, From) })
end)

function CheckUserStatus(address)
    if not Lodash.Contain(WhiteList, address) then -- WhiteList
        return "User " .. address .. " is not allowed to join the event."
    end
    if UploadedUserList[address] then
        return "User " .. address .. " has already called join pool."
    end
    return "User is OK"
end

function BatchAddWhiteList(list)
    for _, v in ipairs(list) do
        Lodash.InsertUnique(WhiteList, v)
    end
end

function BatchRemoveWhiteList(list)
    for _, v in ipairs(list) do
        Lodash.Remove(WhiteList, v)
    end
end

Handlers.add("Check-Permission", "Check-Permission", function (msg)
    local From = msg.FromAddress or msg.From
    msg.reply({ Status = "200", Data = Lodash.Contain(WhiteList, From) })
end)

Handlers.add("Count-WhiteList", "Count-WhiteList", function (msg)
    msg.reply({ Status = "200", Data = tostring(#WhiteList) })
end)

UploadDatasetQueue = UploadDatasetQueue or {}
DatasetStatus = DatasetStatus or {}
function CreateDatasetHandler(msg)
    if UploadedUserList[msg.From] then
        Log.warn(string.format("%s has uploaded dataset before", msg.From))
        msg.reply({ Status = "403", Data = "You have uploaded dataset before." })
        return
    end

    if not Lodash.Contain(WhiteList, msg.From) then -- WhiteList
        Log.warn("User " .. msg.From .. " is not allowed to join the event.")
        msg.reply({ Status = "403", Data = "You are not allowed to join this event." })
        return
    end

    if DatasetStatus[msg.From] ~= nil and DatasetStatus[msg.From].create_pending then
        Log.warn(string.format("%s has pending creation", msg.From))
        msg.reply({ Status = "403", Data = "You have pending creation, please wait for it." })
        return
    end
    local data = Json.decode(msg.Data)

    if UploadedDatasetList[data.hash] then
        Log.warn(string.format("%s has been taken, uploaded by %s", data.hash, msg.From))
        msg.reply({ Status = "403", Data = "Your dataset hash has been taken." })
        return
    end

    Helper.assert_non_empty(data, data.hash, data.list, data.name, msg.PoolID)
    Helper.assert_non_empty_array(data.list)

    if not throttleCheck(msg) then
        return
    end

    Log.info(string.format("%s Creation pending, waiting for syncronizations in Pool", msg.From))
    msg.reply({ Status = "200", Data = "Creation pending, waiting for syncronizations in Pool" })

    DatasetStatus[msg.From] = {
        create_pending = true,
        last_creation = { created_at = Datetime.unix(), updated_at = Datetime.unix(), status = "WAIT_FOR_SYNC" },
    }

    Send({
        Target = Config.Process.Pool,
        Action = "Join-Pool",
        User = msg.From,
        PoolID = msg.PoolID,
        Data = Json.encode({ dataset_hash = data.hash, dataset_name = data.name })
    }).onReply(function (replyMsg)
        Log.trace("Receive reply from the pool " .. replyMsg.From)

        -- because once the data created, the timer will automaticly process the data, so
        -- it is hard to revert, we just ensure it will definately succeed.
        if not DatasetStatus[msg.From].create_pending then
            -- cancel by user.
            Log.trace("Reply message dropped because user canceld the uploading")

            DatasetStatus[msg.From].last_creation.updated_at = Datetime.unix()
            DatasetStatus[msg.From].last_creation.status = "CANCELED"
            return
        end

        local replySwitch = {
            ["403"] = {
                status = "JOIN_POOL_FAILED",
                message = replyMsg,
                func = function ()
                    Log.warn(string.format("%s Join pool failed: %s", msg.From, replyMsg.Data))
                end
            },
            ["200"] = {
                status = "JOIN_SUCCEED",
                message = "Successfully join the pool.",
                func = function ()
                    UploadDatasetQueue[data.hash] = {
                        created_at = Datetime.unix(),
                        list = data.list,
                        embedding = false,
                        pool_id = msg.PoolID,
                        dataset_name = data.name,
                        user = msg.From
                    }
                    Log.info(string.format("%s Create Dataset %s (%s)", msg.From, data.hash, #data.list))
                    if not UploadedUserList[msg.From] then
                        UploadedUserList[msg.From] = true
                    end

                    if not UploadedDatasetList[data.hash] then
                        UploadedDatasetList[data.hash] = true
                    end
                end
            },
            ["default"] = {
                status = "JOIN_POOL_FAILED",
                message = "unknown error",
                func = function ()
                    Log.warn(string.format("%s Join pool failed due to unknown error", msg.From))
                end
            },
        }

        local replyMatch = replySwitch[replyMsg.Status] or replySwitch["default"]
        if replyMatch.func then
            replyMatch.func() -- pay attention to return value to decide if we should continue in the future
        end
        DatasetStatus[msg.From].last_creation.updated_at = Datetime.unix()
        DatasetStatus[msg.From].last_creation.status = replyMatch.status
        DatasetStatus[msg.From].last_creation.message = replyMatch.message
        DatasetStatus[msg.From].create_pending = false
    end)
end

function CancelCreatingDatasetHandler(msg)
    local user = msg.From
    if DatasetStatus[user] then
        DatasetStatus[user].create_pending = false
    else
        DatasetStatus[user] = { create_pending = false }
    end
    msg.replay({ Status = "200" })
end

function GetCreationStatus(msg)
    local user = msg.Data
    msg.reply({ Status = "200", Data = Json.encode(DatasetStatus[user]) })
end

function CountUploadDatasetQueue()
    local count = 0
    for k, v in pairs(UploadDatasetQueue) do
        count = count + 1
    end
    return count
end

function GetUnembededDocumentsHandler(msg)
    for hash, data in pairs(UploadDatasetQueue) do
        msg.reply({
            Status = "200",
            Data = Json.encode({
                dataset_hash = hash,
                documents = data.list,
                pool_id = data.pool_id,
                dataset_name = data.dataset_name,
                user = data.user
            })
        })
        return
    end
    msg.reply({ Status = "200", Data = "{}" })
end

function EmbeddingDataHandler(msg)
    local hash = msg.Data
    local dataset = UploadDatasetQueue[hash]
    if not dataset then
        Log.warn(string.format("Dataset %s not found", hash))
        msg.reply({ Status = "404", Data = "Dataset not found" })
        return
    end
    local now = Datetime.unix()
    Log.info(string.format("Embeded %s documents COSTS %d", hash, now - dataset.created_at))
    UploadDatasetQueue[hash] = nil
    msg.reply({ Status = "200", Data = "Set  " .. hash .. " Embeded" })
end

PromptQueue = PromptQueue or {}
function SearchPromptHandler(msg)
    local PromptReference = msg["X-Reference"] or msg.Reference
    local data = Json.decode(msg.Data)
    Helper.assert_non_empty(data.dataset_hash, data.prompt)
    PromptQueue[PromptReference] = {
        reference = PromptReference,
        dataset_hash = data.dataset_hash,
        prompt = data.prompt,
        sender = msg.From,
        created_at = Datetime.unix()
    }
    Log.info(string.format("%s Prompt %s added successfully", msg.From, PromptReference))
end

function GetToRetrievePromptHandler(msg)
    local prompts = {}
    for _, data in pairs(PromptQueue) do
        table.insert(prompts, data)
        if #prompts >= Config.Embedding.RetrieveSize then
            break
        end
    end
    msg.reply({ Status = "200", Data = Json.encode(prompts) })
end

function RecevicePromptResponseHandler(msg)
    local data = Json.decode(msg.Data)
    for _, item in ipairs(data) do
        Helper.assert_non_empty(item.reference, item.retrieve_result)
        local now = Datetime.unix()
        local prompt = PromptQueue[item.reference]
        if not prompt then
            Log.warn(string.format("Prompt %s not found", item.reference))
            return
        end
        Log.info(string.format("Prompt %s retrieved COSTS %d", item.reference, now - prompt.created_at))
        -- TODO: direct send to Llama
        Send({
            Target = prompt.sender,
            Action = "Search-Prompt-Response",
            ["X-Reference"] = item.reference,
            Data = item.retrieve_result
        })
        PromptQueue[item.reference] = nil
    end
end

Handlers.add("Create-Dataset", "Create-Dataset", CreateDatasetHandler)

Handlers.add("Cancel-Creating-Dataset", "Cancel-Creating-Dataset", CancelCreatingDatasetHandler)

Handlers.add("Get-Unembeded-Documents", "Get-Unembeded-Documents", GetUnembededDocumentsHandler)

Handlers.add("Embedding-Data", "Embedding-Data", EmbeddingDataHandler)

Handlers.add("Search-Prompt", "Search-Prompt", SearchPromptHandler)

Handlers.add("GET-TORETRIEVE-PROMPT", "GET-TORETRIEVE-PROMPT", GetToRetrievePromptHandler)

Handlers.add("Set-Retrieve-Result", "Set-Retrieve-Result", RecevicePromptResponseHandler)

Handlers.add("Get-Creation-Status", "Get-Creation-Status", GetCreationStatus)

function GetDatasetQueueByKeys(keys)
    keys = Json.decode(keys)
    local res = {}
    for _, k in ipairs(keys) do
        if UploadDatasetQueue[k] ~= nil then
            local insert_obj = {}
            insert_obj["created_at"] = UploadDatasetQueue[k].created_at
            insert_obj["list"] = UploadDatasetQueue[k].list
            insert_obj["embedding"] = UploadDatasetQueue[k].embedding
            insert_obj["dataset_hash"] = k
            table.insert(res, insert_obj)
        end
    end
    return Json.encode(res)
end

function ImportWhiteList(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        table.insert(WhiteList, v)
    end
end

function ImportDatasetStatus(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        DatasetStatus[v.dataset_hash] = {
            create_pending = v.create_pending,
            last_creation = v.last_creation
        }
    end
end

function ImportPromptQueue(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        PromptQueue[v.reference] = v
    end
end

function ImportUploadDatasetQueue(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        UploadDatasetQueue[v.dataset_hash] = {
            created_at = v.created_at,
            list = v.list,
            embedding = v.embedding,
        }
    end
end

function ImportUploadDatasetList(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        UploadedDatasetList[v] = true
    end
end

function ImportUploadUserList(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        UploadedUserList[v] = true
    end
end

function ImportWhitelist(data)
    data = Json.decode(data)
    for _, v in ipairs(data) do
        Lodash.InsertUnique(WhiteList, v)
    end
end

function DANGEROUS_CLEAR()
    UploadedUserList = {}
    UploadedDatasetList = {}
    WhiteList = {}
    UploadDatasetQueue = {}
    DatasetStatus = {}
    PromptQueue = {}
end
