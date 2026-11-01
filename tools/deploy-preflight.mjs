import { cloudflareClient } from './cloudflare-api.mjs';

const { request } = cloudflareClient();
const project = process.env.PAGES_PROJECT_NAME ?? 'static-news';
if (!/^[a-z0-9-]+$/.test(project)) throw new Error('Invalid PAGES_PROJECT_NAME');
const details = await request(`pages/projects/${project}`);
if (details.result?.name !== project) throw new Error('Cloudflare returned the wrong Pages project');
await request(`pages/projects/${project}/upload-token`);
console.log(JSON.stringify({ event: 'deploy_preflight_ok', project, productionBranch: details.result.production_branch }));
