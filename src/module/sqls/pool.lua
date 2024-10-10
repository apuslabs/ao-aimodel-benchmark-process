local DB = require("module.utils.db")
local Helper = require("module.utils.helper")
local datetime = require("module.utils.datetime")
local SQL = {}
local json = require("json")

SQL.DATABASE = [[
    CREATE TABLE IF NOT EXISTS participants (
    	dataset_hash TEXT NOT NULL,
        pool_id INTEGER NOT NULL,
    	author TEXT NOT NULL,
    	dataset_name TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        progress REAL,
        score INTEGER,
        rank INTEGER,
        reward INTEGER,
        PRIMARY KEY (dataset_hash, pool_id)
    );
]]

SQL.init = function(client)
    DB:init(client)
    DB:exec(SQL.DATABASE)
end

SQL.CreateParticipant = function(pool_id, author, dataset_hash, dataset_name)
    Helper.assert_non_empty(pool_id, author, dataset_hash, dataset_name)
    return DB:insert("participants", {
        dataset_hash = dataset_hash,
        pool_id = pool_id,
        author = author,
        dataset_name = dataset_name,
        created_at = datetime.unix(),
    })
end

SQL.UpdateRank = function(pool_id, ranks)
    for _, rank in ipairs(ranks) do
        Helper.assert_non_empty(rank.dataset_hash, rank.rank)
        DB:update("participants", {
            rank = rank.rank,
            score = rank.score or 0,
            progress = rank.progress or 0,
            reward = rank.reward or 0,
        }, {
            dataset_hash = rank.dataset_hash,
            pool_id = pool_id,
        })
    end
end

SQL.ImportParticipants = function(data)
    local participants = json.decode(data)
    return DB:batchInsert("participants", participants)
end

SQL.GetParticipant = function(pool_id, dataset_hash)
    return DB:query("participants", { pool_id = pool_id, dataset_hash = dataset_hash })
end

SQL.GetTotalAllParticipants = function()
    local result = DB:nrow(string.format(
        "SELECT COUNT(dataset_hash) AS count FROM participants"))
    return result.count
end

SQL.GetAllParticipantsByPage = function(offset, limit)
    return DB:query("participants", {}, {offset=offset, limit=limit, order="dataset_hash, pool_id"})
end

SQL.GetParticipants = function(pool_id)
    return DB:query("participants", { pool_id = pool_id }, { fields = "dataset_hash, dataset_name" })
end

SQL.CountParticipantsByCreatedTime = function(pool_id, start_time, end_time)
    local result = DB:nrow(string.format(
        "SELECT COUNT(dataset_hash) AS count FROM participants WHERE pool_id = %s AND created_at >= %s AND created_at <= %s",
        pool_id, start_time, end_time))
    return result.count
end

SQL.GetLeaderboard = function(pool_id)
    return DB:query("participants", { pool_id = pool_id }, { order = "rank IS NULL,rank ASC" })
end

SQL.GetTotalParticipants = function(pool_id)
    local countResult = DB:nrow(string.format("SELECT COUNT(dataset_hash) AS count FROM participants WHERE pool_id = %s",
        pool_id))
    return countResult.count
end

SQL.GetTotalRewards = function(pool_id)
    local sumResult = DB:nrow(string.format("SELECT SUM(reward) AS sum FROM participants WHERE pool_id = %s", pool_id))
    return sumResult.sum
end

SQL.GetUserRank = function(pool_id, author)
    local rankResult = DB:queryOne("participants", { pool_id = pool_id, author = author }, { fields = "rank" })
    if rankResult ~= nil then
        return rankResult.rank
    end
    return -1
end

SQL.GetUserReward = function(pool_id, author)
    local rewardResult = DB:queryOne("participants", { pool_id = pool_id, author = author }, { fields = "reward" })
    if rewardResult ~= nil then
        return rewardResult.reward
    end
    return -1
end

SQL.DeleteParticipants = function(pool_id, dataset_hash)
    return DB:exec(string.format("DELETE FROM participants WHERE pool_id = %d AND dataset_hash = '%s'", pool_id,
        dataset_hash))
end

SQL.ClearParticipants = function(pool_id)
    return DB:exec(string.format("DELETE FROM participants WHERE pool_id = %d", pool_id))
end

SQL.CountEvaluatedDatasets = function(pool_id)
    local result = DB:nrow(string.format(
        "SELECT COUNT(dataset_hash) AS count FROM participants WHERE pool_id = %s AND progress >= 1", pool_id))
    return result.count
end

SQL.CountUnEvaluatedDatasets = function(pool_id)
    local result = DB:nrow(string.format(
        "SELECT COUNT(dataset_hash) AS count FROM participants WHERE pool_id = %s AND (progress < 1 OR progress is NULL)", pool_id))
    return result.count
end

return SQL
