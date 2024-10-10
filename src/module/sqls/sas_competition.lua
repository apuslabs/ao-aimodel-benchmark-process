local json = require("json")
local DB = require("module.utils.db")
local Helper = require("module.utils.helper")
local datetime = require("module.utils.datetime")
local SQL = {}

SQL.DATABASE = [[
    CREATE TABLE IF NOT EXISTS questions (
    	id INTEGER PRIMARY KEY,
    	question TEXT,
    	expected_response TEXT
    );
    CREATE TABLE IF NOT EXISTS evaluations (
		id INTEGER PRIMARY KEY,
		participant_dataset_hash TEXT NOT NULL,
		question_id INTEGER NOT NULL,
		sas_score INTEGER,
		created_at INTEGER NOT NULL,
		response_at INTEGER,
		reference TEXT UNIQUE
	);
	CREATE INDEX IF NOT EXISTS idx_evaluations_participant_dataset_hash ON evaluations(participant_dataset_hash);
]]

SQL.init = function(client)
    DB:init(client)
    DB:exec(SQL.DATABASE)
end

SQL.BatchCreateQuestion = function(questions, options)
    options = options or {}
    if options.json_input == true then
        questions = json.decode(questions)
    end
    Helper.assert_non_empty_array(questions)
    local values = {}
    for _, question in ipairs(questions) do
        Helper.assert_non_empty(question.question, question.expected_response)
        local insert_obj = {
            question = question.question,
            expected_response = question.expected_response,
        }
        if options.contain_id == true then
            insert_obj["id"] = question.id
        end
        table.insert(values, insert_obj)
    end
    return DB:batchInsert("questions", values)
end

SQL.GetQuestions = function()
    return DB:query("questions")
end

SQL.GetEvaluations = function(limit, offset)
    return json.encode(DB:query("evaluations", {}, {
        limit = limit,
        offset = offset,
        order = "id"
    }))
end

SQL.GetEvaluationsByDataset = function(dataset_hash)
    return DB:query("evaluations", {
        participant_dataset_hash = dataset_hash,
    })
end

SQL.RecoverTimeoutEvaluations = function()
    return DB:exec([=[
        UPDATE evaluations
        SET reference = NULL, sas_score = NULL, response_at = NULL
        WHERE reference IS NOT NULL AND sas_score IS NULL AND created_at + 7200 < ]=] ..
        datetime.unix() .. [=[;
    ]=])
end

SQL.GetUnfinishedEvaluations = function()
    local ids = DB:nrows("SELECT id from evaluations WHERE sas_score IS NULL;")
    local idarr = {}
    for _, id in ipairs(ids) do
        table.insert(idarr, id.id)
    end
    return json.encode(idarr)
end

SQL.BatchCreateEvaluation = function(evaluations, options)
    options = options or {}
    if options.json_input == true then
        evaluations = json.decode(evaluations)
    end
    print(evaluations, options)
    Helper.assert_non_empty_array(evaluations)
    local values = {}
    for _, evaluation in ipairs(evaluations) do
        Helper.assert_non_empty(evaluation.participant_dataset_hash, evaluation.question_id)
        local insert_obj = {
            participant_dataset_hash = evaluation.participant_dataset_hash,
            question_id = evaluation.question_id,
            created_at = datetime.unix(),
        }
        if options.contain_id == true then
            insert_obj["id"] = evaluation.id
        end
        table.insert(values, insert_obj)
    end
    return DB:batchInsert("evaluations", values)
end

SQL.CreateEvaluationSet = function(participant_dataset_hash)
    local questions = SQL.GetQuestions()
    local evaluations = {}
    for _, question in ipairs(questions) do
        table.insert(evaluations, {
            participant_dataset_hash = participant_dataset_hash,
            question_id = question.id,
        })
    end
    return SQL.BatchCreateEvaluation(evaluations)
end

SQL.GetUnEvaluated = function(limit)
    limit = limit or 1
    return DB:nrows(string.format([[
        SELECT
            e.id,
            e.participant_dataset_hash AS dataset_hash,
            q.question,
            q.expected_response
        FROM evaluations e
        JOIN questions q ON e.question_id = q.id
        WHERE e.reference IS NULL
        ORDER BY e.created_at
        LIMIT %d
    ]], limit))
end

SQL.CountNoScore = function()
    return DB:nrows("SELECT COUNT(id) AS count FROM evaluations WHERE sas_score IS NULL;")
end

SQL.ClearScore = function()
    return DB:exec("UPDATE evaluations SET reference = NULL, sas_score = NULL, response_at = NULL;")
end

SQL.UpdateEvaluationReference = function(id, reference)
    Helper.assert_non_empty(id, reference)
    return DB:update("evaluations", { reference = reference }, {
        id = id,
    })
end

SQL.SetEvaluationResponse = function(reference, sas_score)
    Helper.assert_non_empty(reference, sas_score)
    return DB:update("evaluations", {
        sas_score = tonumber(sas_score),
        response_at = datetime.unix(),
    }, {
        reference = reference,
    })
end

SQL.GetRank = function()
    return DB:nrows([=[
        WITH RankedScores AS (
            SELECT
				participant_dataset_hash,
				SUM(sas_score) AS total_score,
                ROW_NUMBER() OVER (ORDER BY SUM(sas_score) DESC, MIN(created_at)) AS rank,
				COUNT(id) AS total_evaluations,
				SUM(CASE WHEN sas_score IS NULL THEN 0 ELSE 1 END) AS completed_evaluations
            FROM
                evaluations
            GROUP BY
                participant_dataset_hash
        )
        SELECT
            rank,
            participant_dataset_hash AS dataset_hash,
            total_score AS score,
			1.0 * completed_evaluations / total_evaluations AS progress
        FROM
            RankedScores
        ORDER BY
            rank;
    ]=])
end

SQL.SetUnEvaluatedDatasetFinished = function(dataset_hash)
    return DB:update("evaluations", { sas_score = 0 }, {
        participant_dataset_hash = dataset_hash,
        sas_score = "__NULL",
    })
end

SQL.CleanDatasetReference = function(dataset_hash)
    return DB:exec(
        "UPDATE evaluations SET reference = NULL, sas_score = NULL, response_at = NULL WHERE participant_dataset_hash = '" ..
        dataset_hash .. "';")
end

SQL.FindDulplicateDataset = function()
    -- find dataset which count id is > 20
    return DB:nrows([=[
        SELECT
            participant_dataset_hash,
            COUNT(id) AS count
        FROM
            evaluations
        GROUP BY
            participant_dataset_hash
        HAVING
            COUNT(id) > 20;
    ]=])
end

SQL.BatchDeleteEvaluation = function(ids)
    Helper.assert_non_empty_array(ids)
    return DB:exec("DELETE FROM evaluations WHERE id IN (" .. table.concat(ids, ",") .. ");")
end

SQL.GetVersion = function()
    return DB:nrows("SELECT sqlite_version();")
end

SQL.ClearEvaluation = function()
    return DB:exec("DELETE FROM evaluations;")
end

SQL.ClearQuestion = function()
    return DB:exec("DELETE FROM questions;")
end

return SQL
