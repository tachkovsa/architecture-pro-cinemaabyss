import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';

const app = express();

const MONOLITH_URL = process.env.MONOLITH_URL;
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL;
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT, 10) || 0;

if (!MONOLITH_URL) {
  throw new Error('MONOLITH_URL environment variable is required');
}

if (GRADUAL_MIGRATION && !MOVIES_SERVICE_URL) {
  throw new Error('MOVIES_SERVICE_URL environment variable is required when GRADUAL_MIGRATION is true');
}

app.use(/^\/api\/movies(\/.*)?$/, (req, res, next) => {
  let target;
  if (!GRADUAL_MIGRATION) {
    target = MONOLITH_URL;
  } else {
    const percent = Math.random() * 100;
    target = percent < MOVIES_MIGRATION_PERCENT ? MOVIES_SERVICE_URL : MONOLITH_URL;
  }

  return createProxyMiddleware({
    target,
    changeOrigin: true,
    pathRewrite: (path, req) => path
  })(req, res, next);
});

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Proxy service listening on port ${PORT}`);
}); 