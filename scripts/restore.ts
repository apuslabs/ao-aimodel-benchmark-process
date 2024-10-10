import fs from 'fs';
import { msgResult } from './ao/wallet';
import path from 'path';
import { EMBEDDING_PROCESS } from './ao/config';

const POOL_PROCESS = 'mS9PN-9NMH0gk-nxFSA2JRZccmhR92BiRWp7VkoLoGE';
const COMPETITION_PROCESS = 'hrmEo_Hygd-QiawMdQcWH7-ZzWX7Q_c2Qqy4qNN0EYQ';
const DATA_COPY = '20241008230000.csv';
const BATCH_SIZE = 50;

function escapeString(str: string): string {
  const replacements: { [key: string]: string } = {
    '"': '\\"', // 转义双引号
    '\\': '\\\\', // 转义反斜杠
    '\b': '\\b', // 转义退格符
    '\f': '\\f', // 转义换页符
    '\n': '\\n', // 转义换行符
    '\r': '\\r', // 转义回车符
    '\t': '\\t', // 转义制表符
  };

  return str.replace(/["\\\b\f\n\r\t]/g, (match) => replacements[match] || match);
}

function findLatestDataCopy(base_dir: string): string | null {
  const files = fs.readdirSync(base_dir).filter((f) => f != '.gitkeep');
  if (files.length == 0) {
    return null;
  }
  const targetFile = files.sort()[files.length - 1];
  return path.join(base_dir, targetFile);
}

async function singleUpload(records: any[]) {
  const upload_json = JSON.stringify(records);
  const upload_param = `"${escapeString(upload_json)}"`;
  const eval_expression = `SQL.ImportParticipants(${upload_param})`;

  const result = await msgResult(
    POOL_PROCESS,
    {
      Action: 'Eval',
    },
    eval_expression
  );
}

function recoverPoolParticipants() {
  const targetCopy = findLatestDataCopy('backup/pool/participants/');
  if (!targetCopy) {
    console.log('No copy found.');
    return;
  }
  console.log(`Recovering data from copy ${targetCopy} ...`);

  const records = JSON.parse(fs.readFileSync(targetCopy).toString());

  let cur_page = 0;
  while (cur_page * BATCH_SIZE <= records.length - 1) {
    const upload_data = records.slice(cur_page * BATCH_SIZE, Math.min(records.length, (cur_page + 1) * BATCH_SIZE));
    singleUpload(upload_data);

    cur_page += 1;
  }
}

async function recoverCompetition() {
  async function recoverCompetitionQuestions() {
    const targetCopy = findLatestDataCopy('backup/competition/questions/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering competition questions from copy ${targetCopy} ...`);

    const records = JSON.parse(fs.readFileSync(targetCopy).toString());
    const result = await msgResult(
      COMPETITION_PROCESS,
      {
        Action: 'Eval',
      },
      `SQL.BatchCreateQuestion("${escapeString(JSON.stringify(records))}", {json_input=true, contain_id=true})`
    );
  }

  async function recoverCompetitionEvaluations() {
    const targetCopy = findLatestDataCopy('backup/competition/evaluations/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering competition questions from copy ${targetCopy} ...`);

    const records = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const result = await msgResult(
        COMPETITION_PROCESS,
        {
          Action: 'Eval',
        },
        `SQL.BatchCreateEvaluation("${escapeString(
          JSON.stringify(records.slice(cur_page * BATCH_SIZE, Math.min(records.length, (cur_page + 1) * BATCH_SIZE)))
        )}", {json_input=true, contain_id=true})`
      );

      cur_page += 1;
    }
  }
  recoverCompetitionQuestions();
  recoverCompetitionEvaluations();
}

async function recoverEmebedding() {
  async function recoverDatasetStatus() {
    const targetCopy = findLatestDataCopy('backup/embedding/DatasetStatus/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding DatasetStatus from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportDatasetStatus("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportDatasetStatus("${escapeString(JSON.stringify(upload_records))}")`);

      cur_page += 1;
    }
  }

  async function recoverPromptQueue() {
    const targetCopy = findLatestDataCopy('backup/embedding/PromptQueue/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding PromptQueue from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportPromptQueue("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportPromptQueue("${escapeString(JSON.stringify(upload_records))}`);

      cur_page += 1;
    }
  }

  async function recoverUploadDatasetQueue() {
    const targetCopy = findLatestDataCopy('backup/embedding/UploadDatasetQueue/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding UploadDatasetQueue from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportUploadDatasetQueue("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportUploadDatasetQueue("${escapeString(JSON.stringify(upload_records))}`);

      cur_page += 1;
    }
  }

  async function recoverDatasetList() {
    const targetCopy = findLatestDataCopy('backup/embedding/uploadedDatasetList/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding uploadedDatasetList from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportUploadDatasetList("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportUploadDatasetList("${escapeString(JSON.stringify(upload_records))}`);

      cur_page += 1;
    }
  }

  async function recoverUserList() {
    const targetCopy = findLatestDataCopy('backup/embedding/uploadedUserList/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding uploadedUserList from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportUploadUserList("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportUploadUserList("${escapeString(JSON.stringify(upload_records))}`);

      cur_page += 1;
    }
  }

  async function recoverWhiteList() {
    const targetCopy = findLatestDataCopy('backup/embedding/whitelist/');
    if (!targetCopy) {
      console.log('No copy found.');
      return;
    }
    console.log(`Recovering Emebedding whitelist from copy ${targetCopy} ...`);

    const records: any[] = JSON.parse(fs.readFileSync(targetCopy).toString());

    let cur_page = 0;
    while (cur_page * BATCH_SIZE < records.length) {
      const upload_records = records.slice(
        cur_page * BATCH_SIZE,
        Math.min(records.length, (cur_page + 1) * BATCH_SIZE)
      );

      const result = await msgResult(
        EMBEDDING_PROCESS,
        { Action: 'Eval' },
        `ImportWhitelist("${escapeString(JSON.stringify(upload_records))}")`
      );

      console.log(`ImportWhitelist("${escapeString(JSON.stringify(upload_records))}`);

      cur_page += 1;
    }
  }
  // recoverDatasetStatus();
  // recoverPromptQueue();
  // recoverUploadDatasetQueue();
  // recoverDatasetList();
  // recoverUserList();
  recoverWhiteList();
}

(async function main() {
  // recoverPoolParticipants();
  // recoverCompetition();
  recoverEmebedding();
})();
