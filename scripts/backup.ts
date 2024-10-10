import path from 'path';
import { msgResult } from './ao/wallet';
import fs from 'fs';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';

const POOL_PROCESS = 'mS9PN-9NMH0gk-nxFSA2JRZccmhR92BiRWp7VkoLoGE';
const COMPETITION_PROCESS = 'hrmEo_Hygd-QiawMdQcWH7-ZzWX7Q_c2Qqy4qNN0EYQ';
const EMBEDDING_PROCESS = 'ElLRj0tHVFdSPIC5okOTiQcxE5cZ4gmEX_fE-oSH1b0';

// 加载插件
dayjs.extend(utc);
dayjs.extend(timezone);

const PAGE_SIZE = 20;

function getCountFromResult(result: any): number {
  const raw_str = result.Output.data as string;
  const totalRecordsCount = parseInt(raw_str.replace(/^\x1B\[.*\n?/gm, ''));

  return totalRecordsCount;
}

function getRecordsFromResult(result: any): any[] {
  const raw_str = result.Output.data as string;
  const rows = JSON.parse(raw_str);
  return rows as any[];
}

function getNearestHalfHour(): string {
  // 获取当前时间并调整到东八时区
  const now = dayjs().tz('Asia/Shanghai'); // Asia/Shanghai 代表东八时区

  // 获取当前的分钟
  const minutes = now.minute();
  let adjustedTime;

  // 取最近的半小时
  if (minutes >= 30) {
    adjustedTime = now.minute(30).second(0).millisecond(0);
  } else {
    adjustedTime = now.minute(0).second(0).millisecond(0);
  }

  // 格式化为 YYYYMMDDHHmm
  return adjustedTime.format('YYYYMMDDHHmm');
}

// Embedding stores variables in memory
async function getPoolBackup() {
  if (!fs.existsSync('backup/pool/participants/')) {
    fs.mkdirSync('backup/pool/participants/', { recursive: true });
  }
  const result = await msgResult(
    POOL_PROCESS,
    {
      Action: 'Eval',
    },
    'SQL.GetTotalAllParticipants()'
  );
  console.log(result);
  const totalRecordsCount = getCountFromResult(result);

  const target_file_name = 'backup/pool/participants/' + getNearestHalfHour() + '.json';

  if (fs.existsSync(target_file_name)) {
    console.log(`Target copy ${target_file_name} exists.`);
    return;
  }

  const dir = path.dirname(target_file_name);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true }); // 递归创建目录
    console.log('目录已创建:', dir);
  }

  let cur_page = 0;
  let records: any[] = [];
  while (cur_page * PAGE_SIZE < totalRecordsCount) {
    const getRecordsResult = await msgResult(
      POOL_PROCESS,
      {
        Action: 'Eval',
      },
      `Json.encode(SQL.GetAllParticipantsByPage(${cur_page * PAGE_SIZE}, ${PAGE_SIZE}))`
    );
    console.log(`json.encode(SQL.GetAllParticipantsByPage(${cur_page * PAGE_SIZE}, ${PAGE_SIZE}))`);
    console.log(getRecordsResult);
    const rows = getRecordsFromResult(getRecordsResult);

    records = records.concat(rows);
    console.log(records);

    cur_page += 1;
  }
  fs.writeFileSync(target_file_name, JSON.stringify(records, null, 2));
  console.log(`Data has been backed up to the file ${target_file_name}.`);
}

async function getCompetitionBackup() {
  async function getQuestions() {
    if (!fs.existsSync('backup/competition/questions/')) {
      fs.mkdirSync('backup/competition/questions/', { recursive: true });
    }
    const target_file_name = 'backup/competition/questions/' + getNearestHalfHour() + '.json';
    const getQuestionsResult = await msgResult(
      COMPETITION_PROCESS,
      {
        Action: 'Eval',
      },
      `Json.encode(SQL.GetQuestions())`
    );
    const questions = JSON.stringify(JSON.parse(getQuestionsResult.Output.data), null, 2);
    fs.writeFileSync(target_file_name, questions);

    console.log(`Question data has been backed up in ${target_file_name}`);
  }
  async function getEvaluations() {
    console.time('Back up evaluations');
    if (!fs.existsSync('backup/competition/evaluations/')) {
      fs.mkdirSync('backup/competition/evaluations/', { recursive: true });
    }
    const target_file_name = 'backup/competition/evaluations/' + getNearestHalfHour() + '.json';
    let continue_flag = true;
    let cur_page = 0;
    let records: any[] = [];

    while (continue_flag) {
      const getQuestionsResult = await msgResult(
        COMPETITION_PROCESS,
        {
          Action: 'Eval',
        },
        `Json.encode(SQL.GetEvaluations(${PAGE_SIZE}, ${cur_page * PAGE_SIZE}))`
      );
      const res = JSON.parse(JSON.parse(getQuestionsResult.Output.data));
      records = records.concat(res);
      if (res.length < PAGE_SIZE) {
        continue_flag = false;
      }
      console.log(res.length);
      cur_page += 1;
    }

    fs.writeFileSync(target_file_name, JSON.stringify(records, null, 2));
    console.log(`Evaluations data has been backed up in ${target_file_name}`);

    console.timeEnd('Back up evaluations');
  }

  getQuestions();
  getEvaluations();
}

async function getEmbeddingBackup() {
  async function getWhiteList() {
    let cur_page = 0;
    let continue_flag = true;
    let res: any[] = [];
    while (continue_flag) {
      const start_idx = cur_page * PAGE_SIZE;
      const end_idx = (cur_page + 1) * PAGE_SIZE;
      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `Json.encode(ArrayUtils.slice(WhiteList, ${start_idx}, ${end_idx}))`
      );
      console.log(`Json.encode(ArrayUtils.slice(WhiteList, ${start_idx}, ${end_idx}))`);
      const records = JSON.parse(result.Output.data);
      res = res.concat(records);
      if (records.length < PAGE_SIZE) {
        continue_flag = false;
      }
      cur_page += 1;
    }
    const whitelist = JSON.stringify(res, null, 2);

    if (!fs.existsSync('backup/embedding/whitelist/')) {
      fs.mkdirSync('backup/embedding/whitelist/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/whitelist/' + getNearestHalfHour() + '.json';
    fs.writeFileSync(target_file_name, whitelist);

    console.log(`Embedding-Whitelist has been backed up in file ${target_file_name}`);
  }

  async function getUploadedUserList() {
    const result = await msgResult(EMBEDDING_PROCESS, { Action: 'Eval' }, `Json.encode(UploadedUserList)`);

    const uploaded_user_list = JSON.parse(result.Output.data);

    const data = Object.keys(uploaded_user_list);

    if (!fs.existsSync('backup/embedding/uploadedUserList/')) {
      fs.mkdirSync('backup/embedding/uploadedUserList/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/uploadedUserList/' + getNearestHalfHour() + '.json';
    fs.writeFileSync(target_file_name, JSON.stringify(data, null, 2));

    console.log(`Embedding-UploadedUserList has been backed up in file ${target_file_name}`);
  }

  async function getUploadedDatasetList() {
    const result = await msgResult(EMBEDDING_PROCESS, { Action: 'Eval' }, `Json.encode(UploadedDatasetList)`);

    const uploaded_dataset_list = JSON.parse(result.Output.data);

    const data = Object.keys(uploaded_dataset_list);

    if (!fs.existsSync('backup/embedding/uploadedDatasetList/')) {
      fs.mkdirSync('backup/embedding/uploadedDatasetList/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/uploadedDatasetList/' + getNearestHalfHour() + '.json';
    fs.writeFileSync(target_file_name, JSON.stringify(data, null, 2));

    console.log(`Embedding-UploadedDatasetList has been backed up in file ${target_file_name}`);
  }

  async function getDatasetStatus() {
    const result = await msgResult(EMBEDDING_PROCESS, { Action: 'Eval' }, `Json.encode(DatasetStatus)`);

    const dataset_status = JSON.parse(result.Output.data);

    const records = Object.entries(dataset_status).map(([k, v]) => {
      return {
        dataset_hash: k,
        ...(v as any),
      };
    });

    if (!fs.existsSync('backup/embedding/DatasetStatus/')) {
      fs.mkdirSync('backup/embedding/DatasetStatus/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/DatasetStatus/' + getNearestHalfHour() + '.json';
    fs.writeFileSync(target_file_name, JSON.stringify(records, null, 2));

    console.log(`Embedding-DatasetStatus has been backed up in file ${target_file_name}`);
  }

  async function getUploadDatasetQueue() {
    console.time('Get upload dataset queue');
    if (!fs.existsSync('backup/embedding/UploadDatasetQueue/')) {
      fs.mkdirSync('backup/embedding/UploadDatasetQueue/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/UploadDatasetQueue/' + getNearestHalfHour() + '.json';
    // This is very large, use batch to
    const getKeysResult = await msgResult(
      EMBEDDING_PROCESS,
      { Action: 'Eval' },
      `Json.encode(ObjectUtils.getKeys(UploadDatasetQueue))`
    );

    const keys = JSON.parse(getKeysResult.Output.data) as string[];

    let continue_flag = true;
    let cur_page = 0;
    let records: any[] = [];

    while (continue_flag) {
      const _keys = keys.slice(
        Math.min(keys.length - 1, cur_page * PAGE_SIZE),
        Math.min((cur_page + 1) * PAGE_SIZE, keys.length)
      );
      const res = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `GetDatasetQueueByKeys("${JSON.stringify(_keys).replace(/"/g, '\\"')}")`
      );

      records = records.concat(JSON.parse(res.Output.data));
      if (_keys.length < PAGE_SIZE) {
        continue_flag = false;
      }
      cur_page += 1;
    }
    fs.writeFileSync(target_file_name, JSON.stringify(records, null, 2));
    console.log(`Embedding-UploadedDatasetQueue has been backed up in file ${target_file_name}`);
    console.timeEnd('Get upload dataset queue');
  }

  async function getPromptQueue() {
    const result = await msgResult(EMBEDDING_PROCESS, { Action: 'Eval' }, `Json.encode(PromptQueue)`);

    const prompt_queue = JSON.parse(result.Output.data);

    const data = Object.values(prompt_queue);

    if (!fs.existsSync('backup/embedding/PromptQueue/')) {
      fs.mkdirSync('backup/embedding/PromptQueue/', { recursive: true });
    }

    const target_file_name = 'backup/embedding/PromptQueue/' + getNearestHalfHour() + '.json';
    fs.writeFileSync(target_file_name, JSON.stringify(data, null, 2));
    console.log(`Embedding-PromptQueue has been backed up in file ${target_file_name}`);
  }

  getWhiteList();
  // getUploadedUserList();
  // getUploadedDatasetList();
  // getUploadDatasetQueue();
  // getDatasetStatus();
  // getPromptQueue();
}

(async function main() {
  // getPoolBackup();
  // getCompetitionBackup();
  getEmbeddingBackup();
})();
