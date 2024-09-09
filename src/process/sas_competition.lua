local json = require("json")
local sqlite3 = require("lsqlite3")
local SQL = require("module.sqls.sas_competition")
local Config = require("module.utils.config")
local RAGClient = require("module.embedding.client")
local log = require("module.utils.log")
local Datetime = require("module.utils.datetime")
require("module.llama.client")
require("module.utils.helper")

DBClient = DBClient or sqlite3.open_memory()
SQL.init(DBClient)

CircleTimes = CircleTimes or 0
Handlers.add("CronTick", "Cron", function()
    log.trace("Cron Tick")
    if (CircleTimes >= Config.Evaluate.Interval) then
        log.trace("Auto Evaluate")
        Evaluate()
        CircleTimes = 0
    else
        CircleTimes = CircleTimes + 1
    end
end)

function Evaluate()
    local unevaluated = SQL.GetUnEvaluated(Config.Evaluate.BatchSize)
    for _, row in ipairs(unevaluated) do
        local reference = RAGClient.Evaluate(row, function(response, ref)
            SQL.SetEvaluationResponse(ref, response)
        end)
        SQL.UpdateEvaluationReference(row.id, reference)
    end
end

function LoadQuestion(dataStr)
    SQL.BatchCreateQuestion(json.decode(dataStr))
end

function JoinCompetitionHandler(msg)
    SQL.CreateEvaluationSet(msg.Data)
    msg.reply({ Status = "200" })
end

Handlers.add("Join-Competition", "Join-Competition", JoinCompetitionHandler)

Handlers.add("Get-Rank", "Get-Rank", function(msg)
    local rank = SQL.GetRank()
    msg.reply({ Status = "200", Data = json.encode(rank) })
end)

function GetQuestions()
    log.debug(SQL.GetQuestions())
end