const express = require('express');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// --- Config ---
const ALLOWED_DOMAIN = 'ultrahuman.com';
const CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const CALLBACK_URL = process.env.CALLBACK_URL || 'https://powerplugs-dashboard.onrender.com/auth/google/callback';

if (!CLIENT_ID || !CLIENT_SECRET) {
  console.error('ERROR: GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set');
  process.exit(1);
}

// --- Session ---
app.set('trust proxy', 1);
app.use(session({
  secret: process.env.SESSION_SECRET || require('crypto').randomBytes(32).toString('hex'),
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production',
    maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
  },
}));

app.use(passport.initialize());
app.use(passport.session());

// --- Passport Google OAuth ---
passport.use(new GoogleStrategy({
  clientID: CLIENT_ID,
  clientSecret: CLIENT_SECRET,
  callbackURL: CALLBACK_URL,
}, (accessToken, refreshToken, profile, done) => {
  const email = profile.emails?.[0]?.value || '';
  const domain = email.split('@')[1];
  if (domain !== ALLOWED_DOMAIN) {
    return done(null, false, { message: `Only @${ALLOWED_DOMAIN} accounts allowed` });
  }
  return done(null, {
    id: profile.id,
    email,
    name: profile.displayName,
    photo: profile.photos?.[0]?.value,
  });
}));

passport.serializeUser((user, done) => done(null, user));
passport.deserializeUser((user, done) => done(null, user));

// --- Auth middleware ---
function requireAuth(req, res, next) {
  if (req.isAuthenticated()) return next();
  res.redirect('/auth/login');
}

// --- Routes ---

// Login page
app.get('/auth/login', (req, res) => {
  const error = req.query.error;
  res.send(`<!DOCTYPE html>
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Powerplugs Dashboard — Sign In</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0%25' stop-color='%237c5cfc'/%3E%3Cstop offset='100%25' stop-color='%23a855f7'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='32' height='32' rx='6' fill='url(%23g)'/%3E%3Cpath d='M9 8h6.5a5.5 5.5 0 0 1 0 11H13v5H9V8z' fill='white'/%3E%3Cpath d='M13 12v3h2.5a1.5 1.5 0 0 0 0-3H13z' fill='%237c5cfc'/%3E%3C/svg%3E">
</head><body style="margin:0;background:#0a0a0f;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh">
<div style="text-align:center;max-width:380px;width:100%;padding:20px">
  <div style="font-size:32px;font-weight:700;color:#fff;margin-bottom:6px"><span style="color:#7c5cfc">Powerplugs</span> Dashboard</div>
  <div style="font-size:14px;color:#666;margin-bottom:32px">Sign in with your Ultrahuman Google account</div>
  ${error ? `<div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:10px;padding:12px;margin-bottom:20px;font-size:13px;color:#ef4444">${error === 'domain' ? 'Only @ultrahuman.com accounts are allowed' : 'Authentication failed. Try again.'}</div>` : ''}
  <a href="/auth/google" style="display:flex;align-items:center;justify-content:center;gap:10px;padding:13px 24px;background:#fff;border-radius:10px;text-decoration:none;color:#333;font-size:15px;font-weight:500;transition:box-shadow 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.3)" onmouseover="this.style.boxShadow='0 2px 8px rgba(124,92,252,0.3)'" onmouseout="this.style.boxShadow='0 1px 3px rgba(0,0,0,0.3)'">
    <svg width="20" height="20" viewBox="0 0 24 24"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" fill="#4285F4"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/><path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/></svg>
    Sign in with Google
  </a>
  <div style="margin-top:24px;font-size:11px;color:#444">Restricted to @ultrahuman.com accounts</div>
</div>
</body></html>`);
});

// Google OAuth routes
app.get('/auth/google', passport.authenticate('google', {
  scope: ['profile', 'email'],
  hd: ALLOWED_DOMAIN, // hint to Google to show only ultrahuman.com accounts
}));

app.get('/auth/google/callback',
  passport.authenticate('google', { failureRedirect: '/auth/login?error=domain' }),
  (req, res) => res.redirect('/')
);

// Logout
app.get('/auth/logout', (req, res) => {
  req.logout(() => res.redirect('/auth/login'));
});

// --- Protected dashboard ---
app.get('/', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Serve static assets if any (for future use)
app.use(express.static(__dirname, {
  index: false, // don't auto-serve index.html without auth
}));

// User info API (for showing logged-in user in dashboard)
app.get('/auth/user', requireAuth, (req, res) => {
  res.json(req.user);
});

app.listen(PORT, () => {
  console.log(`Dashboard server running on port ${PORT}`);
  console.log(`Auth: Google OAuth (${ALLOWED_DOMAIN} only)`);
});
