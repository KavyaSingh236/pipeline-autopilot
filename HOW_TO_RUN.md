# Pipeline Autopilot — How to Run Locally

## Step 1 — DB Setup (one time, 2 min)

Open **pgAdmin** → right-click "Login/Group Roles" → Create:
- Name: `pipeline`  Password: `pipeline`  ✓ Can login

Right-click "Databases" → Create:
- Name: `pipeline`  Owner: `pipeline`

That's it. The app creates all tables automatically on first start.

---

## Step 2 — First time setup

**Windows:** double-click `setup.bat`

**Mac:** 
```bash
chmod +x setup.sh start.sh
./setup.sh
```

Wait ~3 minutes for npm install to finish.

---

## Step 3 — Run it

**Windows:** double-click `start.bat`

**Mac:** `./start.sh`

---

## What opens

| | URL |
|--|--|
| 🖥 App | http://localhost:3000 |
| ⚡ API docs | http://localhost:8000/docs |

The app auto-seeds synthetic data on first run — no CSV needed.
Pipelines simulate failures every ~12 seconds automatically.
Watch the Control Tower go yellow → click a pipeline → Approve Fix.
