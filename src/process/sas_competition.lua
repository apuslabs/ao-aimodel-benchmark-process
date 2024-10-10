Json = require("json")
local sqlite3 = require("lsqlite3")
SQL = require("module.sqls.sas_competition")
Config = require("module.utils.config")
local RAGClient = require("module.embedding.client")
Log = require("module.utils.log")
require("module.llama.client")
require("module.utils.helper")

DBClient = DBClient or sqlite3.open_memory()
SQL.init(DBClient)

CircleTimes = CircleTimes or 0
Handlers.add("CronTick", "Cron", function ()
    Log.trace("Cron Tick")
    if (CircleTimes >= Config.Evaluate.Interval) then
        Log.trace("Auto Evaluate")
        Evaluate()
        CircleTimes = 0
    else
        CircleTimes = CircleTimes + 1
    end
end)

function Evaluate()
    local unevaluated = SQL.GetUnEvaluated(Config.Evaluate.BatchSize)
    for _, row in ipairs(unevaluated) do
        local reference = RAGClient.Evaluate(row, function (response, ref)
            SQL.SetEvaluationResponse(ref, response)
        end)
        SQL.UpdateEvaluationReference(row.id, reference)
    end
end

function LoadQuestion(dataStr)
    SQL.BatchCreateQuestion(Json.decode(dataStr))
end

function JoinCompetitionHandler(msg)
    SQL.CreateEvaluationSet(msg.Data)
end

Handlers.add("Join-Competition", "Join-Competition", JoinCompetitionHandler)

function GetRank()
    return Json.encode(SQL.GetRank())
end

Handlers.add("Get-Rank", "Get-Rank", function(msg)
    -- msg.reply({ Status = "200", Data = GetRank() })
    Send({ Target = msg.From, Action = "Get-Rank-Response", Data = GetRank() })
end)

function GetQuestions()
    Log.debug(SQL.GetQuestions())
end

function DANGEROUS_CLEAR()
    SQL.ClearEvaluation()
    SQL.ClearQuestion()
end

function SetUnEvaluatedDatasetFinished()
    local rank = SQL.GetRank()
    for _, row in ipairs(rank) do
        if row.progress > 0 and row.progress < 1 then
            SQL.SetUnEvaluatedDatasetFinished(row.dataset_hash)
        end
    end
end

function SetUnstartedDatasetReferenceNull()
    local rank = SQL.GetRank()
    for _, row in ipairs(rank) do
        if row.progress == 0 then
            Log.trace("SetUnstartedDatasetReferenceNull", row.dataset_hash)
            SQL.CleanDatasetReference(row.dataset_hash)
        end
    end
end
