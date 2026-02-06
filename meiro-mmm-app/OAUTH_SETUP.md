# OAuth 2.0 Setup Guide

## Prerequisites

1. **Backend must be running** on `http://localhost:8000`
2. **Frontend must be running** on `http://localhost:5173`
3. **OAuth app credentials** configured in your platform developer consoles

## Environment Variables

Create a `.env` file in the `backend/` directory or set these environment variables:

```bash
# Base URLs (for local development)
BASE_URL=http://localhost:8000
FRONTEND_URL=http://localhost:5173

# Encryption key (use a strong random string in production)
ENCRYPT_KEY=your-strong-encryption-key-change-in-production

# Meta/Facebook OAuth
META_APP_ID=your_meta_app_id
META_APP_SECRET=your_meta_app_secret

# Google Ads OAuth
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret

# LinkedIn OAuth
LINKEDIN_CLIENT_ID=your_linkedin_client_id
LINKEDIN_CLIENT_SECRET=your_linkedin_client_secret
```

## Platform OAuth App Configuration

### Meta/Facebook

1. Go to [Facebook Developers](https://developers.facebook.com/)
2. Create/select your app
3. Add **Facebook Login** product
4. In **Settings** → **Basic**, set:
   - **App Domains**: `localhost`
   - **Website**: `http://localhost:5173`
5. In **Facebook Login** → **Settings**, add to **Valid OAuth Redirect URIs**:
   ```
   http://localhost:8000/api/auth/callback/meta
   ```

### Google Ads

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create/select a project
3. Enable **Google Ads API**
4. Go to **APIs & Services** → **Credentials**
5. Create **OAuth 2.0 Client ID**
6. Set **Authorized redirect URIs**:
   ```
   http://localhost:8000/api/auth/callback/google
   ```

### LinkedIn

1. Go to [LinkedIn Developers](https://www.linkedin.com/developers/)
2. Create/select your app
3. In **Auth** tab, add to **Authorized redirect URLs**:
   ```
   http://localhost:8000/api/auth/callback/linkedin
   ```

## Local Development Setup

1. **Start the backend**:
   ```bash
   cd backend
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

2. **Start the frontend** (in a separate terminal):
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

3. **Verify the proxy is working**:
   - Open browser to `http://localhost:5173`
   - Open DevTools → Network tab
   - Click "Connect Meta Ads"
   - You should see a request to `/api/auth/meta` that gets proxied to the backend

## Troubleshooting

### 404 Error on `/api/auth/meta`

- **Check backend is running**: Open `http://localhost:8000/api/health` in browser
- **Check proxy config**: The Vite proxy should forward `/api/*` to `http://localhost:8000`
- **Check browser console**: Look for CORS or network errors

### "URL not allowed" Error from OAuth Provider

- **Meta**: Ensure `http://localhost:8000/api/auth/callback/meta` is in your app's redirect URIs
- **Google**: Ensure `http://localhost:8000/api/auth/callback/google` is in authorized redirect URIs
- **LinkedIn**: Ensure `http://localhost:8000/api/auth/callback/linkedin` is in authorized redirect URLs

### Environment Variables Not Loading

- Make sure variables are set in the environment where the backend runs
- For Docker: set in `docker-compose.yml` under `api` service `environment:`
- For local: use `.env` file or export variables before running

## Production Setup

For production, update:
- `BASE_URL` to your production API URL
- `FRONTEND_URL` to your production frontend URL
- Redirect URIs in OAuth apps to production URLs
- Use a strong `ENCRYPT_KEY` (generate with: `python -c "import secrets; print(secrets.token_urlsafe(32))"`)







