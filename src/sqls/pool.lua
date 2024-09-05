local DB = require("utils.db")
local Helper = require("utils.helper")
local datetime = require("utils.datetime")
local SQL = {}

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
    Helper.assert_array(ranks)
    local values = {}
    for _, rank in ipairs(ranks) do
        Helper.assert_non_empty(rank.dataset_hash, rank.rank, rank.score, rank.progress, rank.reward)
        DB:update("participants", {
            rank = rank.rank,
            score = rank.score,
            progress = rank.progress,
            reward = rank.reward,
        }, {
            dataset_hash = rank.dataset_hash,
            pool_id = pool_id,
        })
    end
end

SQL.GetParticipant = function(pool_id, dataset_hash)
    return DB:query("participants", { pool_id = pool_id, dataset_hash = dataset_hash })
end

SQL.GetParticipants = function(pool_id)
    return DB:query("participants", { pool_id = pool_id }, { fields = "dataset_hash" })
end

SQL.GetLeaderboard = function(pool_id)
    return DB:query("participants", { pool_id = pool_id }, { order = "rank ASC" })
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
    local rankResult = DB:query("participants", { pool_id = pool_id, author = author }, { fields = "rank" })
    return rankResult.rank
end

SQL.GetUserReward = function(pool_id, author)
    local rewardResult = DB:query("participants", { pool_id = pool_id, author = author }, { fields = "reward" })
    return rewardResult.reward
end

return SQL
