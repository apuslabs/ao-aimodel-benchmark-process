local json = require("json")
local ao = require('.ao')
local sqlite3 = require("lsqlite3")
SQL = require("module.sqls.pool")
Log = require("module.utils.log")
local Lodash = require("module.utils.lodash")
local Config = require("module.utils.config")
local Helper = require("module.utils.helper")
local Datetime = require("module.utils.datetime")

DBClient = DBClient or sqlite3.open_memory()
SQL.init(DBClient)

CompetitionPools = CompetitionPools or {}
local function getOngoingCompetitions()
    local pools = {}
    for id, pool in pairs(CompetitionPools) do
        local metadata = json.decode(pool.metadata)
        if not metadata.competition_time then
            Log.error("Competition time not found in metadata")
            return {}
        end
        local startTime = tonumber(metadata.competition_time["start"])
        local endTime = tonumber(metadata.competition_time["end"])
        local now = Datetime.unix()
        if now >= startTime and now <= endTime then
            pools[id] = pool
        end
    end
    return pools
end


Handlers.add("Get-Competitions", "Get-Competitions", function(msg)
    msg.reply({ Status = "200", Data = json.encode(Lodash.keys(CompetitionPools)) })
end)

Handlers.add("Get-Competition", "Get-Competition", function(msg)
    local poolId = tonumber(msg.Data)
    msg.reply({ Status = "200", Data = json.encode(CompetitionPools[poolId]) })
end)

Handlers.add("Get-Participants", "Get-Datasets", function(msg)
    local poolId = tonumber(msg.Data)
    msg.reply({ Status = "200", Data = json.encode(SQL.GetParticipants(poolId)) })
end)

Handlers.add("Get-Leaderboard", { Action = "Get-Leaderboard" }, function(msg)
    local poolId = tonumber(msg.Data)
    msg.reply({ Status = "200", Data = json.encode(SQL.GetLeaderboard(poolId)) })
end)

Handlers.add("Get-Dashboard", "Get-Dashboard", function(msg)
    local From = msg.FromAddress or msg.From
    local poolID = tonumber(msg.Data)
    local rank = 0
    local rewarded_tokens = 0
    if (From ~= "" and From ~= "1234") then
        rank = SQL.GetUserRank(poolID, From)
        rewarded_tokens = SQL.GetUserReward(poolID, From)
    end
    msg.reply({
        Status = 200,
        Data = json.encode({
            participants = SQL.GetTotalParticipants(poolID),
            granted_reward = SQL.GetTotalRewards(poolID),
            rank = rank,
            rewarded_tokens = rewarded_tokens
        })
    })
end)

APUS_BALANCE = APUS_BALANCE or 0
function UpdateBalance()
    Send({ Target = Config.Process.Token, Action = "Balance" })
end

Handlers.add("Update-Balance", { From = Config.Process.Token, Account = ao.id }, function(msg)
    APUS_BALANCE = tonumber(msg.Balance)
end)
function Transfer(receipent, quantity)
    Send({
        Target = Config.Process.Token,
        Tags = {
            { name = "Action",    value = "Transfer" },
            { name = "Recipient", value = receipent },
            { name = "Quantity",  value = tostring(quantity) }
        }
    })
end

LatestPoolID = LatestPoolID or 1000

function CreatePoolHandler(msg)
    -- TODO: semantic params
    Helper.assert_non_empty(msg["X-Title"], msg["X-Process-ID"],
        msg["X-MetaData"])

    CreatePool(msg["X-Title"], msg.Quantity, msg["X-Process-ID"], msg["X-MetaData"])
    Send({
        Target = msg.Sender,
        Action = "Create-Pool-Notice",
        Status = 200,
        Data = LatestPoolID
    })
end

function CreatePool(title, reward_pool, process_id, metadata)
    LatestPoolID = LatestPoolID + 1
    CompetitionPools[LatestPoolID] = {
        owner = ao.id,
        title = title,
        reward_pool = reward_pool,
        process_id = process_id,
        metadata = metadata
    }
    return LatestPoolID
end

Handlers.add("Create-Pool", { Action = "Credit-Notice", From = Config.Process.Token }, CreatePoolHandler)

local poolTimeCheck = function(poolID)
    local metadata = json.decode(CompetitionPools[poolID].metadata)
    local startTime = metadata.competition_time["start"]
    local endTime = metadata.competition_time["end"]
    local now = Datetime.unix()
    return now >= tonumber(startTime) and now <= tonumber(endTime)
end
local throttleCheck = Helper.throttleCheckWrapper(Config.Pool.JoinThrottle)
function JoinPoolHandler(msg)
    local data = json.decode(msg.Data)
    Helper.assert_non_empty(msg.PoolID, data.dataset_hash, data.dataset_name)
    local poolID = tonumber(msg.PoolID)
    if not poolTimeCheck(poolID) then
        msg.reply({ Status = 403, Data = "The event has ended, can't join in." })
    end
    if not throttleCheck(msg) then
        return
    end

    SQL.CreateParticipant(poolID, msg.From, data.dataset_hash, data.dataset_name)
    msg.reply({ Status = 200, Data = "Join Success" })
    Send({
        Target = CompetitionPools[poolID].process_id,
        Action = "Join-Competition",
        Data = data.dataset_hash
    })
end

Handlers.add("Join-Pool", "Join-Pool", JoinPoolHandler)

Reward = { 35000, 20000, 10000, 5000, 5000, 5000, 5000, 5000, 5000, 5000 }
local function allocateReward(rank)
    if rank <= 10 then
        return Reward[rank] * 2
    elseif rank <= 200 then
        return 300 * 2
    else
        return 0
    end
end
function GetRank(poolID)
    Send({
        Target = CompetitionPools[poolID].process_id,
        Action = "Get-Rank"
    }).onReply(function(msg)
        local ranks = json.decode(msg.Data)
        Log.info("Update Rank ", poolID, ranks)
        for i in ipairs(ranks) do
            ranks[i].reward = allocateReward(i)
        end
        SQL.UpdateRank(poolID, ranks)
    end)
end

CircleTimes = CircleTimes or 0
function AutoUpdateLeaderboard()
    if CircleTimes >= Config.Pool.LeaderboardInterval then
        local ongoingCompetitions = getOngoingCompetitions()

        for id, pool in pairs(ongoingCompetitions) do
            Log.trace("Auto Update Leaderboard ", pool.title)
            GetRank(id)
        end
        CircleTimes = 0
    else
        CircleTimes = CircleTimes + 1
    end
end

Handlers.add("CronTick", "Cron", function()
    Log.trace("Cron Tick")
    AutoUpdateLeaderboard()
end)

WhiteList = WhiteList or {}
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

Handlers.add("Count-WhiteList", "Count-WhiteList", function(msg)
    msg.reply({ Status = 200, Data = #WhiteList })
end)

Handlers.add("Check-Permission", "Check-Permission", function(msg)
    local From = msg.FromAddress or msg.From
    msg.reply({ Status = 200, Data = Lodash.Contain(WhiteList, From) })
end)

Handlers.add("Participants-Statistic", "Participants-Statistic", function(msg)
    local poolID = tonumber(msg.Data)
    local now = Datetime.unix()
    local lastHour = now - 3600
    local lastDay = now - 86400
    local lastHourParticipants = SQL.CountParticipantsByCreatedTime(poolID, lastHour, now)
    local lastDayParticipants = SQL.CountParticipantsByCreatedTime(poolID, lastDay, now)
    local totalParticipants = SQL.GetTotalParticipants(poolID)

    msg.reply({
        Status = 200,
        Data = json.encode({
            last_hour = lastHourParticipants,
            last_day = lastDayParticipants,
            total = totalParticipants
        })
    })
end)

function DANGEROUS_CLEAR()
    SQL.ClearParticipants(1002)
end
