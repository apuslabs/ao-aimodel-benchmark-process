import { EMBEDDING_PROCESS, EMBEDDING_SERVICE } from './ao/config';
import { dryrun, dryrunDebug, msgResult } from './ao/wallet';
import axios from 'axios';

axios.defaults.baseURL = EMBEDDING_SERVICE;

async function getUnembeddedDocuments() {
  try {
    const result = await dryrun(EMBEDDING_PROCESS, {
      Action: 'Get-Unembeded-Documents',
    });
    const data = result.Messages?.[0]?.Data ?? '[]';
    return JSON.parse(data);
  } catch (e) {
    console.error('Failed to retrieve documents', e);
  }
}

interface Dataset {
  dataset_hash: string;
  documents: { content: string }[];
  pool_id: string;
  dataset_name: string;
  user: string;
}

function decodeMeta(meta: string) {
  try {
    return JSON.parse(meta);
  } catch (e) {
    // console.error("Failed to decode meta", e);
    return {};
  }
}

async function embedDocuments(d: Dataset) {
  const list = [
    {
      dataset_id: d.dataset_hash,
      documents: d.documents,
      pool_id: d.pool_id,
      user: d.user,
      dataset_name: d.dataset_name,
    },
  ];
  if (!list.length) {
    console.warn('No documents to embed', d.dataset_hash);
    return 0;
  }
  const res = await axios.post('/create-dataset', {
    list,
  });
  return res.data?.count ?? 0;
}

async function setDocumentsEmbedded(d: Dataset) {
  // exact ids from embeddingDocs
  const result = await msgResult(EMBEDDING_PROCESS, { Action: 'Embedding-Data' }, d.dataset_hash);
  const data = result.Messages?.[0]?.Data ?? 0;
  return data;
}

function executeWithRetry(asyncFunc: () => Promise<void>, intervalMs: number) {
  async function wrapper() {
    try {
      await asyncFunc();
    } catch (e) {
      console.error('An error occurred during execution', e);
    } finally {
      setTimeout(wrapper, intervalMs);
    }
  }
  // Initial call to start the process
  setTimeout(wrapper, intervalMs);
}

async function embeddingDocs() {
  const dataset: Dataset = await getUnembeddedDocuments();
  if (dataset && dataset.dataset_hash && dataset.documents?.length) {
    try {
      const embeddingDocs = await embedDocuments(dataset);
      console.log(`Embedded ${embeddingDocs} documents`);
      const replyMsg = await setDocumentsEmbedded(dataset);
      console.log(replyMsg);
    } catch (e: any) {
      console.error('Failed to embed documents', e?.response?.data ?? e?.message ?? e);
    }
  }
}

interface Prompt {
  reference: string;
  sender: string;
  dataset_hash: string;
  prompt: string;
  retrieve_result?: string;
}

const PromptPool: Record<string, Prompt> = {};
const ErrorPool: Record<string, Prompt> = {};

async function getToRetrievePrompt() {
  try {
    const result = await dryrun(EMBEDDING_PROCESS, {
      Action: 'GET-TORETRIEVE-PROMPT',
    });
    const data = result.Messages?.[0]?.Data ?? '[]';
    const prompts = JSON.parse(data);
    prompts.forEach((prompt: any) => {
      if (!PromptPool[prompt.reference]) {
        PromptPool[prompt.reference] = {
          ...prompt,
          ...PromptPool[prompt.reference],
        };
      }
    });
  } catch (e) {}
}

async function retrievePrompt() {
  const toRetrievePrompts = Object.values(PromptPool).filter((p) => !p.retrieve_result);
  if (!toRetrievePrompts.length) return;
  // only process 1 prompts at a time
  const toRetrievePrompts50 = toRetrievePrompts.slice(0, 1);
  // group by dataset_hash
  const groupedPrompts = toRetrievePrompts50.reduce(
    (acc, doc) => {
      (acc[doc.dataset_hash] = acc[doc.dataset_hash] || []).push({
        prompt: doc.prompt,
        reference: doc.reference,
      });
      return acc;
    },
    {} as Record<string, any[]>
  );
  // group to list
  const list = Object.entries(groupedPrompts).map(([dataset_id, prompts]) => ({
    dataset_id,
    prompts,
  }));
  const res = await axios.post('/retrieve-data', { list });
  res.data.map(({ reference, result }: any) => {
    const prompt = PromptPool[reference];
    if (prompt) {
      prompt.retrieve_result = result || 'Null';
    }
  });
}

async function setPromptRetrieved() {
  const toSetPrompt = Object.values(PromptPool).filter((p) => !!p.retrieve_result);
  if (!toSetPrompt.length) return;
  try {
    await msgResult(
      EMBEDDING_PROCESS,
      { Action: 'Set-Retrieve-Result' },
      toSetPrompt.map((v) => ({
        reference: v.reference,
        retrieve_result: v.retrieve_result,
      }))
    );
    console.log(`Set retrieve result for ${toSetPrompt.map((v) => v.reference).join(',')}`);
    for (const prompt of toSetPrompt) {
      delete PromptPool[prompt.reference];
    }
  } catch {
    for (const prompt of toSetPrompt) {
      ErrorPool[prompt.reference] = prompt;
      delete PromptPool[prompt.reference];
    }
  }
}

function autoRetrievePrompts() {
  executeWithRetry(async () => {
    await getToRetrievePrompt();
    await setPromptRetrieved();
  }, 1000);
  executeWithRetry(async () => {
    await retrievePrompt();
  }, 100);
}

async function main() {
  executeWithRetry(embeddingDocs, 5000);
  autoRetrievePrompts();
}

main();
