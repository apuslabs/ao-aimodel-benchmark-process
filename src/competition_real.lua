local json = require("json")
local ao = require('ao')
local sqlite3 = require("lsqlite3")
Llama = require("@sam/Llama-Herder")

DB = DB or nil
CompetitonPools = CompetitonPools or {}
TokenProcessId = "lpJ5Edz_8DbNnVDL0XdbsY9vCOs45NACzfI4jvo4Ba8"
EmbeddingProcessId = "hMEUOgWi97d9rGT-lHNCbm937bTypMq7qxMWeaUnMLo"
Phi3Template = [[<|system|>
                %s<|end|>
                <|user|>
                %s<|end|>]]

SasSystemPrompt = [[You are a helpful assistant that can compute the SAS(semantic answer similarity) score.
                    You can compute a score between 0~100 based on the SAS, 0 means totally different, 100 means almost the same.
                    Now the user will send you:
                    1. one Question
                    2. the Context for the question
                    3. an ExpectedResponse
                    pls:
                    1. generate a Response for the Question based on the provided Context.
                    2. compute the SAS score between the provided ExpectedResponse with the Response generated.
                    **Important**You must return as this format: {<the-sas-score>}]]
PRIZE_BALANCE = PRIZE_BALANCE or 0
CompetitonPoolId = 1001

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
      INSERT INTO datasets(question, expected_response) VALUES ('%s', '%s');
    ]],
  INSERT_PARTICIPANTS = [[
      INSERT INTO participants (author, upload_dataset_name, participant_dataset_hash) VALUES('%s', '%s', '%s');
    ]],
  INSERT_EVALUATIONS = [[
      INSERT INTO evaluations (participant_id, participant_dataset_hash, dataset_id, question, correct_answer) VALUES('%s', '%s', '%s','%s', '%s');
    ]],
  ADD_REWARDED_TOKENS = [[
      UPDATE participants SET rewarded_tokens = rewarded_tokens + '%d'
    ]],
  FIND_ALL_DATASET = [[
      SELECT id, question, expected_response FROM datasets;
    ]],
  FIND_USER_REWARDED_TOKENS = [[
      SELECT rewarded_tokens as rewardedTokens from participants WHERE author = '%s';
    ]],
  GET_UNEVALUATED_EVALUATIONS = [[
      SELECT * FROM evaluations WHERE inference_start_time IS NULL LIMIT %d;
    ]],
  START_EVALUATION = [[
      UPDATE evaluations SET inference_start_time = CURRENT_TIMESTAMP, inference_reference = '%d' WHERE id = '%d';
    ]],
  END_EVALUATION = [[
    UPDATE evaluations SET inference_end_time = CURRENT_TIMESTAMP, prediction = '%s' WHERE inference_reference = '%s';
    ]],
  GET_EVALUATION_BY_REFERENCE = [[
      SELECT * FROM evaluations WHERE inference_reference = '%s';
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

SearchPromptReference = 0
Handlers.add(
  "Evaluate",
  Handlers.utils.hasMatchingTag("Action", "Evaluate"),
  function(msg)
    local limit = tonumber(msg.Data) or 2
    for row in DB:nrows(string.format(SQL.GET_UNEVALUATED_EVALUATIONS, limit)) do
      local ragData = string.format('{"dataset_hash": "%s","prompt":"%s"}', row.participant_dataset_hash, row.question)
      SearchPromptReference = SearchPromptReference + 1
      ao.Send({
        Target = EmbeddingProcessId,
        Data = ragData,
        Tags = {
          { name = "Action",    value = "Search-Prompt" },
          { name = "Reference", value = SearchPromptReference }
        },
      })
      DB:exec(string.format(
        SQL.START_EVALUATION,
        SearchPromptReference, row.id
      ))
    end
  end
)

Handlers.add(
  "Search-Prompt-Response",
  Handlers.utils.hasMatchingTag("Action", "Search-Prompt-Response"),
  function(msg)
    local evaluationReference = msg.Tags.Reference
    for row in DB:nrows(string.format(SQL.GET_EVALUATION_BY_REFERENCE, evaluationReference)) do
      local data = json.decode(msg.Data)

      local sentences = " Question: " ..
          row.question .. ", Context: " .. data.prompt .. ", ExpectedResponse: " .. row.correct_answer
      local prompt = string.format(Phi3Template, SasSystemPrompt, sentences)
      Llama.run(prompt, 3, function(sasScore)
        print("Sas score:" .. sasScore .. "\n")
        DB:exec(SQL.UPDATE_SCORE, extractSasScore(sasScore), evaluationReference)
      end)
      -- end)
    end
  end
)

Handlers.add(
  "Balance-Response",
  function(msg)
    return msg.Tags.from == TokenProcessId and
        msg.Tags.Account == ao.id and msg.Tags.Balance ~= nil
  end,
  function(msg)
    print("Updated Balance:" .. msg.Tags.Balance)
    PRIZE_BALANCE = msg.Tags.Balance
  end
)

Handlers.add(
  "Load-Data",
  Handlers.utils.hasMatchingTag("Action", "Load-Data"),
  function(msg)
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
        msg.From == TokenProcessId
  end,
  function(msg)
    -- TODO
    local title = msg.Tags["X-Title"]
    local description = msg.Tags["X-Description"]
    local prizePool = msg.Tags["X-Prize-Pool"]
    local metaData = msg.Tags["X-MetaData"]

    CompetitonPools[CompetitonPoolId] = {
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

    DB:exec(string.format(
      SQL.INSERT_PARTICIPANTS,
      author,
      datasetName,
      datasetHash
    ))
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


function UpdateBalance()
  ao.Send({
    Target = TokenProcessId,
    Tags = {
      { name = "Action", value = "Balance" }
    }
  })
end

Handlers.add(
  "Get-Pool",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool"),
  function(msg)
    local pool = CompetitonPools[CompetitonPoolId]
    ao.send({
      Target = msg.From,
      Tags = {
        { name = "Action", value = "Get-Pool-Response" },
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
  if rank <= 10 then
    return reward[rank] * PRIZE_BALANCE / 100
  else
    return 300
  end
end

local function computeNeedRewarded(author, amount)
  for rewardTokens in DB:nrows(string.format(SQL.FIND_USER_REWARDED_TOKENS, author)) do
    return amount - rewardTokens
  end
  return amount
end

Handlers.add(
  "Allocate-Rewards",
  Handlers.utils.hasMatchingTag("Action", "Allocate-Rewards-Response"),
  function(msg)
    local rank = 0
    for item in DB:nrows(SQL.TOTAL_SCORES_BY_PARTICIPANT) do
      rank = rank + 1
      local amount = computeReward(rank)
      amount = computeNeedRewarded(item.participant_id)
      if PRIZE_BALANCE < amount then
        print("Balance is not enough, balance: " .. PRIZE_BALANCE .. " want: " .. amount)
      elseif amount > 0 then
        PRIZE_BALANCE = PRIZE_BALANCE - amount
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
    local tempParticipants = 0
    local tempRewardedTokens = 0
    local tempRank = 0
    local tempReward = 0

    print("Get-Dashboard begin")
    for row in DB:nrows(SQL.TOTAL_PARTICIPANT_REWARDED_TOKENS) do
      tempParticipants = row.total_participants
      tempRewardedTokens = row.total_rewarded_tokens
    end

    local sender = msg.Tags["Sender"]
    print("sender" .. Dump(sender))
    for row in DB:nrows(string.format(SQL.FIND_USER_REWARDED_TOKENS, sender)) do
      tempReward = row.rewardedTokens
      print("tempReward" .. Dump(tempReward))
    end

    for row in DB:nrows(string.format(SQL.FIND_USER_RANK, sender)) do
      tempRank = row.rank
      print("temp Rank" .. Dump(tempRank))
    end

    print("Get-Dashboard END")
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
    for row in DB:nrows(query) do
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

Handlers.add(
  "DEBUG-DB",
  Handlers.utils.hasMatchingTag("Action", "DEBUG-DB"),
  function(msg)
    print("Data insertion begin")
    DB:exec [[
  INSERT INTO participants (author, upload_dataset_name, participant_dataset_hash, rewarded_tokens) VALUES
  ('Author 1', 'Dataset 1', 'hash1', 10),
  ('Author 2', 'Dataset 2', 'hash2', 20),
  ('Author 3', 'Dataset 3', 'hash3', 30),
  ('fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY', 'Dataset 4', 'hash4', 40),
  ('Author 5', 'Dataset 5', 'hash5', 50),
  ('Author 6', 'Dataset 6', 'hash6', 60),
  ('Author 7', 'Dataset 7', 'hash7', 70),
  ('Author 8', 'Dataset 8', 'hash8', 80),
  ('Author 9', 'Dataset 9', 'hash9', 90),
  ('Author 10', 'Dataset 10', 'hash10', 100),
  ('Author 11', 'Dataset 11', 'hash11', 110),
  ('Author 12', 'Dataset 12', 'hash12', 120),
  ('Author 13', 'Dataset 13', 'hash13', 130),
  ('Author 14', 'Dataset 14', 'hash14', 140),
  ('Author 15', 'Dataset 15', 'hash15', 150),
  ('Author 16', 'Dataset 16', 'hash16', 160),
  ('Author 17', 'Dataset 17', 'hash17', 170),
  ('Author 18', 'Dataset 18', 'hash18', 180),
  ('Author 19', 'Dataset 19', 'hash19', 190),
  ('Author 20', 'Dataset 20', 'hash20', 200);
]]

    -- 插入 datasets 表的测试数据
    DB:exec [[
  INSERT INTO datasets (context, question, expected_response) VALUES
  ('Context 1', 'Question 1', 'Response 1'),
  ('Context 2', 'Question 2', 'Response 2'),
  ('Context 3', 'Question 3', 'Response 3'),
  ('Context 4', 'Question 4', 'Response 4'),
  ('Context 5', 'Question 5', 'Response 5'),
  ('Context 6', 'Question 6', 'Response 6'),
  ('Context 7', 'Question 7', 'Response 7'),
  ('Context 8', 'Question 8', 'Response 8'),
  ('Context 9', 'Question 9', 'Response 9'),
  ('Context 10', 'Question 10', 'Response 10'),
  ('Context 11', 'Question 11', 'Response 11'),
  ('Context 12', 'Question 12', 'Response 12'),
  ('Context 13', 'Question 13', 'Response 13'),
  ('Context 14', 'Question 14', 'Response 14'),
  ('Context 15', 'Question 15', 'Response 15'),
  ('Context 16', 'Question 16', 'Response 16'),
  ('Context 17', 'Question 17', 'Response 17'),
  ('Context 18', 'Question 18', 'Response 18'),
  ('Context 19', 'Question 19', 'Response 19'),
  ('Context 20', 'Question 20', 'Response 20');
]]

    -- 插入 evaluations 表的测试数据
    DB:exec [[
  INSERT INTO evaluations (participant_id, participant_dataset_hash, dataset_id, question, correct_answer, prediction, prediction_sas_score, inference_start_time, inference_end_time, inference_reference) VALUES
  (1, 'hash1', 1, 'Question 1', 'Answer 1', 'Prediction 1', 10, '2023-01-01 10:00:00', '2023-01-01 10:05:00', 'Reference 1'),
  (1, 'hash2', 2, 'Question 2', 'Answer 2', 'Prediction 2', 20, '2023-01-02 11:00:00', '2023-01-02 11:05:00', 'Reference 2'),
  (1, 'hash3', 3, 'Question 3', 'Answer 3', 'Prediction 3', 30, '2023-01-03 12:00:00', '2023-01-03 12:05:00', 'Reference 3'),
  (1, 'hash4', 4, 'Question 4', 'Answer 4', 'Prediction 4', 40, '2023-01-04 13:00:00', '2023-01-04 13:05:00', 'Reference 4'),
  (4, 'hash5', 5, 'Question 5', 'Answer 5', 'Prediction 5', 50, '2023-01-05 14:00:00', '2023-01-05 14:05:00', 'Reference 5'),
  (4, 'hash6', 6, 'Question 6', 'Answer 6', 'Prediction 6', 60, '2023-01-06 15:00:00', '2023-01-06 15:05:00', 'Reference 6'),
  (4, 'hash7', 7, 'Question 7', 'Answer 7', 'Prediction 7', 70, '2023-01-07 16:00:00', '2023-01-07 16:05:00', 'Reference 7'),
  (4, 'hash8', 8, 'Question 8', 'Answer 8', 'Prediction 8', 80, '2023-01-08 17:00:00', '2023-01-08 17:05:00', 'Reference 8'),
  (9, 'hash9', 9, 'Question 9', 'Answer 9', 'Prediction 9', 90, '2023-01-09 18:00:00', '2023-01-09 18:05:00', 'Reference 9'),
  (10, 'hash10', 10, 'Question 10', 'Answer 10', 'Prediction 10', 100, '2023-01-10 19:00:00', '2023-01-10 19:05:00', 'Reference 10'),
  (11, 'hash11', 11, 'Question 11', 'Answer 11', 'Prediction 11', 110, '2023-01-11 20:00:00', '2023-01-11 20:05:00', 'Reference 11'),
  (12, 'hash12', 12, 'Question 12', 'Answer 12', 'Prediction 12', 120, '2023-01-12 21:00:00', '2023-01-12 21:05:00', 'Reference 12'),
  (13, 'hash13', 13, 'Question 13', 'Answer 13', 'Prediction 13', 130, '2023-01-13 22:00:00', '2023-01-13 22:05:00', 'Reference 13'),
  (14, 'hash14', 14, 'Question 14', 'Answer 14', 'Prediction 14', 140, '2023-01-14 23:00:00', '2023-01-14 23:05:00', 'Reference 14'),
  (15, 'hash15', 15, 'Question 15', 'Answer 15', 'Prediction 15', 150, '2023-01-15 00:00:00', '2023-01-15 00:05:00', 'Reference 15'),
    (16, 'hash16', 16, 'Question 16', 'Answer 16', 'Prediction 16', 160, '2023-01-16 01:00:00', '2023-01-16 01:05:00', 'Reference 16'),
    (17, 'hash17', 17, 'Question 17', 'Answer 17', 'Prediction 17', 170, '2023-01-17 02:00:00', '2023-01-17 02:05:00', 'Reference 17'),
    (18, 'hash18', 18, 'Question 18', 'Answer 18', 'Prediction 18', 180, '2023-01-18 03:00:00', '2023-01-18 03:05:00', 'Reference 18'),
    (19, 'hash19', 19, 'Question 19', 'Answer 19', 'Prediction 19', 190, '2023-01-19 04:00:00', '2023-01-19 04:05:00', 'Reference 19'),
    (20, 'hash20', 20, 'Question 20', 'Answer 20', 'Prediction 20', 200, '2023-01-20 05:00:00', '2023-01-20 05:05:00', 'Reference 20');
]]


    print("Data insertion complete")

    print("start debug DB")

    for row in DB:nrows("select count(*) as cnt from participants;") do
      print("participants Row number" .. Dump(row))
    end

    for row in DB:nrows("select * from participants;") do
      print("participants" .. Dump(row))
    end


    for row in DB:nrows("select count(*) as cnt from datasets;") do
      print("datasets Row number" .. Dump(row))
    end

    for row in DB:nrows("select * from datasets;") do
      print("datasets" .. Dump(row))
    end

    for row in DB:nrows("select count(*) as cnt from evaluations;") do
      print("evaluations Row number" .. Dump(row))
    end
  end
)
