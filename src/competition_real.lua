local json = require("json")
local ao = require('ao')
local sqlite3 = require("lsqlite3")
Llama = require("@sam/Llama-Herder")

DB = DB or nil
CompetitonPools = CompetitonPools or {}
TokenProcessId = ""
RAG_PROCESS_ID = "hMEUOgWi97d9rGT-lHNCbm937bTypMq7qxMWeaUnMLo"
Phi3Template = [[<|system|>
                %s<|end|>
                <|user|>
                %s<|end|>]]

SasSystemPrompt = [[You are a helpful assistant that can compute the SAS(semantic answer similarity) metrics.
                    You can compute a score between 0~100 based on the SAS, 0 means totally different, 100 means almost the same.
                    Now the user will send you two sentences(sentenceA and sentenceB), please return the SAS score of them.
                    **Important**You must return as this format: {<the-sas-score>}.]]

Handlers.add(
  "Init",
  Handlers.utils.hasMatchingTag("Action", "Init"),
  function()
    DB = sqlite3.open_memory()

    DB:exec [[
            CREATE TABLE participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    author TEXT NOT NULL,
                    upload_dataset_name TEXT NOT NULL,
                    upload_dataset_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    participant_dataset_hash TEXT,
                    rewarded_tokens INTEGER DEFAULT 0
                );

            CREATE TABLE datasets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    context TEXT,
                    question TEXT,
                    expected_response TEXT
            );

            CREATE TABLE evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id INTEGER NOT NULL,
                    participant_dataset_hash TEXT,
                    dataset_id INTEGER NOT NULL,
                    question TEXT NOT NULL,
                    correct_answer TEXT NOT NULL,
                    prediction TEXT,
                    prediction_sas_score INTEGER,
                    inference_start_time DATETIME,
                    inference_end_time DATETIME,
                    inference_reference TEXT,
                    FOREIGN KEY (participant_id) REFERENCES participants(id)
                    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
                );
        ]]
    print("OK")
  end
)

local SQL = {
  INSERT_DATASET = [[
      INSERT INTO datasets(question, expected_response) VALUES ("%s", "%s");
    ]],
  INSERT_PARTICIPANTS = [[
      INSERT INTO participants (author, upload_dataset_name, participant_dataset_hash) VALUES('%s', '%s', '%s');
    ]],
  INSERT_EVALUATIONS = [[
      INSERT INTO evaluations (participant_id, participant_dataset_hash, dataset_id, question, correct_answer);
    ]],
  ADD_REWARDED_TOKENS = [[
      UPDATE participants SET rewarded_tokens = rewarded_tokens + '%s'
    ]],
  -- GET_DATASET_ID = [[SELECT id FROM datasets WHERE author = '%s' AND participant_dataset_hash = '%s']],
  FIND_ALL_DATASET = [[
      SELECT id, question, expected_response FROM datasets;
    ]],
  FIND_USER_REWARDED_TOKENS = [[
      SELECT rewarded_tokens as rewardedTokens from participants WHERE author = '%s'
    ]],
  GET_UNEVALUATED_EVALUATIONS = [[
      SELECT * FROM evaluations WHERE inference_start_time IS NULL LIMIT %d;
    ]],
  START_EVALUATION = [[
      UPDATE evaluations SET inference_start_time = CURRENT_TIMESTAMP, inference_reference = '%s' WHERE id = '%s';
    ]],
  END_EVALUATION = [[
    UPDATE evaluations SET inference_end_time = CURRENT_TIMESTAMP, prediction = '%s' WHERE inference_reference = '%s';
    ]],
  UPDATE_SCORE = [[
    UPDATE evaluations SET prediction_sas_score = '%s' WHERE inference_reference = '%s';
    ]],
  TOTAL_SCORES_BY_PARTICIPANT = [[
      SELECT participant_id as author, SUM(prediction_sas_score) as score, COUNT(*) as count
      , SUM(prediction_sas_score) /  COUNT(*) as averageScore FROM evaluations
      GROUP BY participant_id
      ORDER BY averageScore DESC
    ]],
  TOTAL_PARTICIPANT_REWARDED_TOKENS = [[
    SELECT
    COUNT(*) AS total_participants,
    SUM(rewarded_tokens) AS total_rewarded_tokens
    FROM
    participants;]],
  TOTAL_PARTICIPANTS_RANK = [[
WITH RankedScores AS (
    SELECT
        e.participant_id AS participant_id,
        p.upload_dataset_name AS dataset_name,
        p.upload_dataset_time AS dataset_upload_time,
        d.id AS dataset_id,
        p.author,
        p.rewarded_tokens AS granted_reward,
        SUM(e.prediction_sas_score) AS total_score,
        COUNT(e.prediction_sas_score) AS count,
        SUM(e.prediction_sas_score) / COUNT(e.prediction_sas_score) AS averageScore,
        ROW_NUMBER() OVER (ORDER BY SUM(e.prediction_sas_score) / COUNT(e.prediction_sas_score) DESC) AS rank
    FROM
        evaluations e
    JOIN
        participants p ON e.participant_id = p.id
    JOIN
        datasets d ON e.dataset_id = d.id
    GROUP BY
        e.participant_id
)
SELECT
    rank,
    dataset_id,
    dataset_name,
    dataset_upload_time,
    averageScore AS score,
    author,
    granted_reward
FROM
    RankedScores
ORDER BY
    rank;
]],
  FIND_USER_RANK = [[
WITH RankedScores AS (
    SELECT
        e.participant_id AS participant_id,
        p.upload_dataset_name AS dataset_name,
        p.upload_dataset_time AS dataset_upload_time,
        d.id AS dataset_id,
        p.author,
        p.rewarded_tokens AS granted_reward,
        SUM(e.prediction_sas_score) AS total_score,
        COUNT(e.prediction_sas_score) AS count,
        SUM(e.prediction_sas_score) / COUNT(e.prediction_sas_score) AS averageScore,
        ROW_NUMBER() OVER (ORDER BY SUM(e.prediction_sas_score) / COUNT(e.prediction_sas_score) DESC) AS rank
    FROM
        evaluations e
    JOIN
        participants p ON e.participant_id = p.id
    JOIN
        datasets d ON e.dataset_id = d.id
    GROUP BY
        e.participant_id
)
SELECT
    rank,
    dataset_id,
    dataset_name,
    dataset_upload_time,
    averageScore AS score,
    author,
    granted_reward
FROM
    RankedScores
WHERE
    author = '%s'
ORDER BY
    rank;
]]
}

Handlers.add(
  "Evaluate",
  Handlers.utils.hasMatchingTag("Action", "Evaluate"),
  function(msg)
    local limit = tonumber(msg.Data) or 2
    for row in DB:nrows(string.format(SQL.GET_UNEVALUATED_EVALUATIONS, limit)) do
      local ragData = string.format('{"dataset_hash": "%s","prompt":"%s"}', row.participant_dataset_hash, row.question)
      ao.Send({
        Target = RAG_PROCESS_ID,
        Action = "Search-Prompt",
        Data = ragData
      })

      local reference = Llama.Reference
      print("Inference: " .. row.prompt .. "\n")
      Llama.run(row.prompt, 1, function(answer)
        print("Answer: " .. answer .. "\n")
        DB:exec(string.format(
          SQL.END_EVALUATION,
          answer, reference
        ))
        local expectedResponse = row.correct_answer
        local sentences = " sentenceA: " .. answer .. ", sentenceB:" .. expectedResponse .. "."
        local prompt = string.format(Phi3Template, SasSystemPrompt, sentences)
        Llama.run(prompt, 1, function(sasScore)
          print("Sas score:" .. sasScore .. "\n")
          DB:exec(SQL.UPDATE_SCORE, extractSasScore(sasScore), reference)
        end)
      end)
      DB:exec(string.format(
        SQL.START_EVALUATION,
        reference, row.id
      ))
    end
  end
)

Handlers.add(
  "Load-Data",
  Handlers.utils.hasMatchingTag("Action", "Load-QA-Data"),
  function(msg)
    -- Handlers.utils.reply("start Load-Data!!!!!!")(msg)
    local data = msg.Data
    assert(data ~= nil, "Data is nil")
    local DataSets = json.decode(data)
    for _, DataSetItem in ipairs(DataSets) do
      local query = string.format(
        SQL.INSERT_DATASET,
        DataSetItem.context,
        DataSetItem.expected_response[1]
      )
      DB:exec(query)
    end
    print('ok')
  end
)


Handlers.add(
  "Create-Pool",
  function(msg)
    return msg.Tags.Action == "Credit-Notice" and
        msg.From == ApusTokenProcess
  end,
  function(msg)
    -- TODO
    local title = msg.Tags["X-Title"]
    local description = msg.Tags["X-Description"]
    local prizePool = msg.Tags["X-Prize-Pool"]
    local metaData = msg.Tags["X-MetaData"]

    CompetitonPools[1001] = {
      title = title,
      description = description,
      prizePool = prizePool,
      metaData = metaData
    }
    print(CompetitonPools)
    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Create-Pool-Response" },
        { name = "status", value = "200" }
      }
    })
    print("OK")
  end
)

local function addParticipants(author, datasetName, datasetHash)
  DB:exec(string.format(
    SQL.INSERT_PARTICIPANTS,
    author,
    datasetName,
    datasetHash
  ))
end

local function initBenchmarkRecords(participantId, participantDatasetHash)
  for row in DB:nrows(string.format(SQL.FIND_ALL_DATASET)) do
    DB:exec(string.format(SQL.INSERT_EVALUATIONS,
      participantId, participantDatasetHash, row.id, row.question, row.expected_response))
  end
end
Handlers.add(
  "Join-Pool",
  Handlers.utils.hasMatchingTag("Action", "Join-Pool"),
  function(msg)
    local data = json.decode(msg.Data)
    local author = msg.From
    local datasetHash = data.dataset_hash
    local datasetName = data.dataset_name

    addParticipants(author, datasetHash, datasetName)
    initBenchmarkRecords(author, datasetHash)

    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Join-Pool-Response" },
        { name = "status", value = "200" }
      }
    })
    print("OK")
  end
)

Handlers.add(
  "Get-Pool",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool"),
  function(msg)
    -- TODO

    local id = 1001
    local pool = CompetitonPools[id]
    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Create-Pool-Response" },
        { name = "status", value = "200" }
      },
      Data = json.encode({
        title = pool['title'],
        prize_pool = pool['prizePool'],
        meta_data = pool['metaData']
      })
    })
    print("OK")
  end
)

local reward = { 35, 20, 10, 5, 5, 5, 5, 5, 5, 5 }

local function computeReward(rank)
  local prizaTotal = 100000
  if rank <= 10 then
    return reward[rank] * prizaTotal / 100
  else
    return 300
  end
end

local function adjustUserRewarded(author, amount)
  for rewaredTokens in DB:nrows(string.format(SQL.FIND_USER_REWARDED_TOKENS, author)) do
    return amount - rewaredTokens
  end
  return 0
end

Handlers.add(
  "Allocate-Rewards",
  Handlers.utils.hasMatchingTag("Action", "Allocate-Rewards-Response"),
  function(msg)
    local rank = 0
    for item in DB:nrows(SQL.TOTAL_SCORES_BY_PARTICIPANT) do
      rank = rank + 1
      local amount = computeReward(rank)
      amount = adjustUserRewarded(item.participant_id)
      if amount >= 0 then
        transfer(item.participant_id, amount)
        DB:exec(SQL.ADD_REWARDED_TOKENS, amount)
      end
    end

    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Allocate-Rewards-Response" },
        { name = "status", value = "200" }
      }
    })
    print("OK")
  end
)

function transfer(author, amount)
  ao.send({
    Target = TokenProcessId,
    Tags = {
      { name = "Action",    value = "Transfer" },
      { name = "Recipient", value = author },
      { name = "Quantity",  value = amount }
    }
  })
end

Handlers.add(
  "Get-Dashboard",
  Handlers.utils.hasMatchingTag("Action", "Get-Dashboard"),
  function(msg)
    -- TODO @Json
    local tempParticipants = 0
    local tempRewardedTokens = 0
    local tempRank = 0
    local tempReward = 0
    for row in DB:nrows(SQL.TOTAL_PARTICIPANT_REWARDED_TOKENSSELECT) do
      tempParticipants = row.total_participants
      tempRewardedTokens = row.total_rewarded_tokens
      print("Total Participants: " .. row.total_participants)
      print("Total Rewarded Tokens: " .. row.total_rewarded_tokens)
    end

    for row in DB:nrows(string.format(SQL.FIND_USER_REWARDED_TOKENS, msg.author)) do
      tempReward = row.rewarded_tokens
    end

    for row in DB:nrows(string.format(SQL.FIND_USER_RANK, msg.author)) do
      tempRank = row.tempRank
    end

    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Get-Dashboard-Response" },
        { name = "status", value = "200" }
      },
      Data = json.encode({
        participants = tempParticipants,
        granted_reward = tempRewardedTokens,
        my_rank = tempRank,
        my_reward = tempReward
      })
    })
    print("OK")
  end
)

Handlers.add(
  "Get-Leaderboard",
  Handlers.utils.hasMatchingTag("Action", "Get-Leaderboard"),
  function(msg)
    local data = {}
    local query = SQL.TOTAL_PARTICIPANTS_RANK
    for row in db:nrows(query) do
      table.insert(data, {
        rank = row.rank,
        dataset_id = row.dataset_id,
        dataset_name = row.dataset_name,
        dataset_upload_time = row.dataset_upload_time,
        score = row.score,
        author = row.author,
        granted_reward = row.granted_reward
      })
    end

    -- local data = json.encode({
    --   {
    --     rank = 1,
    --     dataset_id = 10,
    --     dataset_name = "a good dataset",
    --     dataset_upload_time = 1722254056,
    --     score = 65,
    --     author = "ewewrerr",
    --     granted_reward = 0
    --   },
    --   {
    --     rank = 2,
    --     dataset_id = 12,
    --     dataset_name = "a bad dataset",
    --     dataset_upload_time = 1722254059,
    --     score = 60,
    --     author = "ewewrerreewdddd",
    --     granted_reward = 0
    --   }
    -- });
    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Get-Leaderboard-Response" },
        { name = "status", value = "200" }
      },
      Data = json.encode(data)
    })
    print("OK")
  end
)
