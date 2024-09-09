local Worker = {}

Worker.Utils = [=[
local aos2 = {}

aos2.replyMsg = function(msg, replyMsg)
    replyMsg.Target = msg["Reply-To"] or (replyMsg.Target or msg.From)
    replyMsg["X-Reference"] = msg["X-Reference"] or msg.Reference
    replyMsg["X-Origin"] = msg["X-Origin"] or nil

    return ao.send(replyMsg)
end
]=]

Worker.Evaluate = Worker.Utils .. [=[
-- Module: XcWULRSWWv_bmaEyx4PEOFf4vgRSVCP9vM5AucRvI40

Colors = {
    red = "\27[31m",
    green = "\27[32m",
    blue = "\27[34m",
    reset = "\27[0m",
    gray = "\27[90m"
}

WorkerType = "Evaluate"

local json = require("json")

ModelID = ModelID or "ISrbGzQot05rs_HKC08O_SmkipYQnqgB1yC3mjZZeEo"
Llama = Llama or nil
RouterID = RouterID or "972kot-Duchcz6lkGD9EFnm4O2-k_0xT_QjxxNRySPM"

InferenceAllowList = {
    [RouterID] = true,
    [ao.id] = true
}

DefaultMaxResponse = DefaultMaxResponse or 40

SystemPrompt = [[You are evaluating dataset quality. Follow these steps:
1. Assume the role of Sam Williams, Arweave founder, to understand the topic.
2. Formulate your answer to the question based on context and question.
3. As a robot, compare your answer with the expected response. Score semantic similarity from integer between 0 and 10 (0 = no similarity, 10 = almost identical).
  - If context is null, score based on existing knowledge.

Input JSON format:
```json
{"question": "...","context": "<QA of Sam's Tweet>","expected_response": "..."}
```
  - "context" may contain multiple lines or be null.

Output: Always respond in this JSON format:
```json
{"score": <integer_score_0_to_10>}
```
]]


function PrimePromptText(systemPrompt)
    return [[<|system|>
]] .. systemPrompt .. [[<|end|>
<|user|>
]]
end

function Init()
    Llama = require("llama")
    Llama.logLevel = 4

    print("Loading model: " .. ModelID)
    Llama.load("/data/" .. ModelID)

    local initialPrompt = PrimePromptText(SystemPrompt)
    print("Initial Prompt: " .. initialPrompt)
    Llama.setPrompt(initialPrompt)

    print("Save state")
    Llama.saveState()
end

function CompletePromptText(userPrompt)
    return userPrompt .. [[<|end|>
<|assistant|>]]
end

DefaultResponse = {
    Score = -1,
}

function ProcessPetition(userPrompt)
    local additionalPrompt = CompletePromptText(userPrompt)
    Llama.add(additionalPrompt)

    local responseJson = nil

    local responseBuilder = ""
    for i = 1, DefaultMaxResponse do
        responseBuilder = responseBuilder .. Llama.next()

        local responseJsonMatch = string.match(responseBuilder, ".*({.*}).*")
        if responseJsonMatch then
            responseJson = json.decode(responseJsonMatch)
            break
        end

        -- if end of <|endoftext|> or <|end|>, stop
        if string.match(responseBuilder, ".*<|end|>.*") or string.match(responseBuilder, ".*<|endoftext|>.*") or string.match(responseBuilder, ".*<|user|>.*") or string.match(responseBuilder, ".*<|assistant|>.*") or string.match(responseBuilder, ".*<|system|>.*") then
            break
        end
    end

    if not responseJson or not responseJson.score then
        print("Unusable response: " .. responseBuilder)
        return DefaultResponse
    end

    -- Parse the grade
    local scoreNumber = tonumber(responseJson.score)
    if not scoreNumber then
        print("Invalid grade: " .. responseJson.score)
        return DefaultResponse
    end

    -- Clamp the grade
    scoreNumber = math.min(10, math.max(-1, scoreNumber))

    return {
        Score = scoreNumber,
    }
end

Handlers.add(
    "Init",
    Handlers.utils.hasMatchingTag("Action", "Init"),
    function(msg)
        if msg.From ~= ao.id then
            return print("Init not allowed: " .. msg.From)
        end

        ModelID = msg.Tags["Model-ID"] or ModelID
        DefaultMaxResponse = msg.Tags["Max-Response"] or DefaultMaxResponse
        Init()
        ao.send({
            Target = RouterID,
            Tags = {
                Action = "Init-Response",
                WorkerType = WorkerType,
            },
        })
    end
)

Handlers.add(
    "Inference",
    Handlers.utils.hasMatchingTag("Action", "Inference"),
    function(msg)
        if not InferenceAllowList[msg.From] then
            print("Inference not allowed: " .. msg.From)
            return
        end

        local userPrompt = msg.Data
        local response = ProcessPetition(userPrompt)

        local score = response.Score
        print("[" .. Colors.gray .. "INFERENCE" .. Colors.reset .. " ]" ..
            " From: " .. Colors.blue .. msg.From .. Colors.reset ..
            " | Reference: " .. Colors.blue .. msg.Tags["Reference"] .. Colors.reset ..
            " | Score: " .. Colors.blue .. score .. Colors.reset)

        aos2.replyMsg(msg, { Data = tostring(score) })

        Llama.loadState()
    end
)
]=]

Worker.Chat = Worker.Utils .. [=[
-- Module: XcWULRSWWv_bmaEyx4PEOFf4vgRSVCP9vM5AucRvI40

Colors = {
    red = "\27[31m",
    green = "\27[32m",
    blue = "\27[34m",
    reset = "\27[0m",
    gray = "\27[90m"
}

WorkerType = "Chat"

ModelID = ModelID or "ISrbGzQot05rs_HKC08O_SmkipYQnqgB1yC3mjZZeEo"
Llama = Llama or nil
RouterID = RouterID or "972kot-Duchcz6lkGD9EFnm4O2-k_0xT_QjxxNRySPM"

InferenceAllowList = {
    [RouterID] = true,
    [ao.id] = true
}

DefaultMaxResponse = DefaultMaxResponse or 40

SystemPrompt = [[
You are Sam Williams, Arweave founder, answer question based on the context.

Input JSON format:
```json
{"question": "...","context": "<QA of Sam's Tweet>"}
```
  - "context" may contain multiple lines or be null.

Output:
1. Plain text, MAX 25 words, ]] .. DefaultMaxResponse .. [[ tokens.
2. Answer concisely in one sentence, no line breaks, stop when complete.
2. If context is null, use existing knowledg, but don't invent facts.
]]


function PrimePromptText(systemPrompt)
    return [[<|system|>
]] .. systemPrompt .. [[<|end|>
<|user|>
]]
end

function Init()
    Llama = require("llama")
    Llama.logLevel = 4

    print("Loading model: " .. ModelID)
    Llama.load("/data/" .. ModelID)

    local initialPrompt = PrimePromptText(SystemPrompt)
    print("Initial Prompt: " .. initialPrompt)
    Llama.setPrompt(initialPrompt)

    print("Save state")
    Llama.saveState()
end

function CompletePromptText(userPrompt)
    return userPrompt .. [[<|end|>
<|assistant|>]]
end

DefaultResponse = {
    Answer = "",
}

function ProcessPetition(userPrompt)
    local additionalPrompt = CompletePromptText(userPrompt)
    Llama.add(additionalPrompt)

    local responseBuilder = ""
    for i = 1, DefaultMaxResponse do
        responseBuilder = responseBuilder .. Llama.next()

        -- if end of <|endoftext|> or <|end|>, stop
        if string.match(responseBuilder, ".*<|.*") then
            responseBuilder = string.gsub(responseBuilder, "<|.*", "")
            break
        end
    end

    return {
        Answer = responseBuilder,
    }
end

Handlers.add(
    "Init",
    Handlers.utils.hasMatchingTag("Action", "Init"),
    function(msg)
        if msg.From ~= ao.id then
            return print("Init not allowed: " .. msg.From)
        end

        ModelID = msg.Tags["Model-ID"] or ModelID
        DefaultMaxResponse = msg.Tags["Max-Response"] or DefaultMaxResponse
        Init()
        ao.send({
            Target = RouterID,
            Tags = {
                Action = "Init-Response",
                WorkerType = WorkerType,
            },
        })
    end
)

Handlers.add(
    "UpdateSystemPrompt",
    Handlers.utils.hasMatchingTag("Action", "Update-System-Prompt"),
    function(msg)
        if msg.From ~= ao.id then
            return print("UpdateSystemPrompt not allowed: " .. msg.From)
        end

        local initialPrompt = PrimePromptText(SystemPrompt)
        print("Updated System Prompt: " .. initialPrompt)
        Llama.setPrompt(initialPrompt)
        Llama.saveState()
    end
)

Handlers.add(
    "Inference",
    Handlers.utils.hasMatchingTag("Action", "Inference"),
    function(msg)
        if not InferenceAllowList[msg.From] then
            print("Inference not allowed: " .. msg.From)
            return
        end

        local userPrompt = msg.Data
        local response = ProcessPetition(userPrompt)

        local answer = response.Answer
        print("[" .. Colors.gray .. "INFERENCE" .. Colors.reset .. " ]" ..
            " From: " .. Colors.blue .. msg.From .. Colors.reset ..
            " | Reference: " .. Colors.blue .. msg.Tags["Reference"] .. Colors.reset ..
            " | Answer: " .. Colors.blue .. answer .. Colors.reset)

        aos2.replyMsg(msg, { Data = answer })

        Llama.loadState()
    end
)
]=]

local LlamaModule = "XcWULRSWWv_bmaEyx4PEOFf4vgRSVCP9vM5AucRvI40"

Worker.SpawnEvaluator = function()
    local msg = Spawn(LlamaModule, {
        Tags = {
            Authority = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY"
        }
    }).receive()
    Send({
        Target = msg.Process,
        Action = "Eval",
        Data = Worker.Evaluate .. [[Send({ Target = RouterID, Action = "Code-Loaded" })]]
    })
    Receive({ Action = "Code-Loaded", From = msg.Process })
    Send({ Target = msg.Process, Action = "Init" })
end

Worker.SpawnChat = function()
    local msg = Spawn(LlamaModule, {
        Tags = {
            Authority = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY"
        }
    }).receive()
    Send({
        Target = msg.Process,
        Action = "Eval",
        Data = Worker.Chat .. [[Send({ Target = RouterID, Action = "Code-Loaded" })]]
    })
    Receive({ Action = "Code-Loaded", From = msg.Process })
    Send({ Target = msg.Process, Action = "Init" })
end

return Worker