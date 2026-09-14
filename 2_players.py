import json
import uuid
import asyncio
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI()

# --- In-Memory Storage ---
rooms: Dict[str, dict] = {}
room_players: Dict[str, Set[str]] = {}

class UsernameRequest(BaseModel):
    username: str

# --- Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.room_connections: Dict[str, Dict[str, WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str, username: str):
        await websocket.accept()
        if room_id not in self.room_connections:
            self.room_connections[room_id] = {}
        self.room_connections[room_id][username] = websocket

    def disconnect(self, room_id: str, username: str):
        if room_id in self.room_connections and username in self.room_connections[room_id]:
            del self.room_connections[room_id][username]
            if not self.room_connections[room_id]:
                del self.room_connections[room_id]
                rooms.pop(room_id, None)
                room_players.pop(room_id, None)

    async def broadcast(self, room_id: str, message: dict):
        if room_id in self.room_connections:
            dead = []
            for user, ws in self.room_connections[room_id].items():
                try:
                    await ws.send_json(message)
                except Exception:
                    dead.append(user)
            for u in dead:
                self.disconnect(room_id, u)

manager = ConnectionManager()

# --- API Endpoints ---

@app.post("/create-room")
async def create_room(req: UsernameRequest):
    if not req.username or len(req.username) < 3:
        raise HTTPException(status_code=400, detail="نام باید حداقل ۳ حرف باشد")
        
    room_id = str(uuid.uuid4())[:6]
    rooms[room_id] = {
        "host": req.username,
        "players": [req.username],
        "status": "waiting",
        "scores": {req.username: 0}
    }
    room_players[room_id] = {req.username}
    return {"room_id": room_id}

@app.get("/join-room/{room_id}")
async def join_room(room_id: str, username: str = Query(..., min_length=3)):
    if room_id not in rooms:
        raise HTTPException(status_code=404, detail="اتاق پیدا نشد")
    if len(rooms[room_id]["players"]) >= 2:
        raise HTTPException(status_code=400, detail="اتاق پر است")
    if username in room_players.get(room_id, set()):
        raise HTTPException(status_code=409, detail="این نام قبلا استفاده شده")
        
    rooms[room_id]["players"].append(username)
    rooms[room_id]["scores"][username] = 0
    room_players.setdefault(room_id, set()).add(username)
    return {"status": "joined", "players": rooms[room_id]["players"]}

# --- WebSocket Logic ---

@app.websocket("/ws/game/{room_id}/{username}")
async def game_ws(websocket: WebSocket, room_id: str, username: str):
    if room_id not in rooms or username not in rooms[room_id]["players"]:
        await websocket.close(code=1008, reason="Invalid")
        return

    await manager.connect(websocket, room_id, username)
    room = rooms[room_id]
    
    await manager.broadcast(room_id, {"type": "player_joined", "players": room["players"]})

    if len(room["players"]) == 2 and room["status"] == "waiting":
        room["status"] = "countdown"
        asyncio.create_task(start_countdown(room_id))

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            
            if msg["type"] == "move":
                await manager.broadcast(room_id, {"type": "update_pos", "username": username, "x": msg["x"], "y": msg["y"]})
            elif msg["type"] == "shoot":
                await manager.broadcast(room_id, {"type": "new_bullet", "username": username, "x": msg["x"], "y": msg["y"]})
            elif msg["type"] == "enemy_killed":
                if username in room["scores"]:
                    room["scores"][username] += 10
                    await manager.broadcast(room_id, {"type": "score_update", "scores": room["scores"]})
                    if room["scores"][username] >= 100:
                        room["status"] = "finished"
                        await manager.broadcast(room_id, {"type": "game_over", "winner": username, "scores": room["scores"]})
    except WebSocketDisconnect:
        manager.disconnect(room_id, username)
        if room_id in rooms:
             await manager.broadcast(room_id, {"type": "player_left", "players": rooms[room_id]["players"]})

async def start_countdown(room_id: str):
    if room_id not in rooms: return
    for i in range(3, 0, -1):
        await manager.broadcast(room_id, {"type": "countdown", "value": i})
        await asyncio.sleep(1)
    if room_id in rooms:
        rooms[room_id]["status"] = "playing"
        await manager.broadcast(room_id, {"type": "game_start"})
        asyncio.create_task(enemy_spawner(room_id))

async def enemy_spawner(room_id: str):
    while room_id in rooms and rooms[room_id]["status"] == "playing":
        await asyncio.sleep(1.5)
        eid = str(uuid.uuid4())[:6]
        await manager.broadcast(room_id, {"type": "spawn_enemy", "id": eid, "x": (hash(eid) % 700) + 50, "y": 0})

# --- Frontend ---
@app.get("/")
async def root():
    return HTMLResponse(GAME_HTML)

GAME_HTML = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>Space Shooter Lobby</title>
    <style>
        body { margin: 0; background: #0a0a12; color: white; font-family: Tahoma, sans-serif; overflow: hidden; display: flex; justify-content: center; align-items: center; height: 100vh; }
        canvas { border: 2px solid #333; background: #000; display: none; box-shadow: 0 0 30px rgba(0,255,0,0.1); }
        .panel { text-align: center; background: rgba(20,20,30,0.95); padding: 2rem; border-radius: 15px; border: 1px solid #444; width: 350px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); animation: fadeIn 0.3s ease; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        input { width: 85%; padding: 12px; margin: 8px 0; border-radius: 6px; border: 1px solid #555; background: #1a1a24; color: white; direction: ltr; text-align: center; font-size: 1.1rem;}
        button { width: 95%; padding: 12px; margin-top: 10px; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 1rem; transition: 0.2s; }
        button:disabled { opacity: 0.6; cursor: wait; filter: grayscale(0.8); }
        .btn-green { background: #2ecc71; color: white; } .btn-green:hover:not(:disabled) { background: #27ae60; }
        .btn-blue { background: #3498db; color: white; } .btn-blue:hover:not(:disabled) { background: #2980b9; }
        .hidden { display: none !important; }
        #share-box { background: #000; padding: 10px; border: 1px dashed #666; margin: 15px 0; word-break: break-all; font-size: 0.85rem; direction: ltr; color: #aaa; border-radius: 4px; cursor: pointer; }
        #share-box:hover { color: #fff; border-color: #888; }
        #overlay { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; justify-content: center; align-items: center; font-size: 8rem; font-weight: bold; color: white; text-shadow: 0 0 20px black; pointer-events: none; z-index: 100; }
        #hud { position: absolute; top: 10px; left: 10px; right: 10px; display: flex; justify-content: space-between; pointer-events: none; direction: ltr; }
        .score-card { background: rgba(0,0,0,0.6); padding: 5px 12px; border-radius: 4px; margin-right: 5px; border: 1px solid #333; }
        h2 { margin-top: 0; color: #2ecc71; }
        .note { font-size: 0.75rem; color: #888; margin-top: 15px; line-height: 1.4; }
        .player-list { text-align: right; margin: 15px 0; background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; }
        .player-item { padding: 5px 0; border-bottom: 1px solid #333; display: flex; justify-content: space-between; }
        .player-item:last-child { border-bottom: none; }
        .you-tag { color: #2ecc71; font-size: 0.8rem; background: rgba(46,204,113,0.1); padding: 2px 6px; border-radius: 4px; }
    </style>
</head>
<body>

    <!-- Login Panel -->
    <div id="auth-panel" class="panel">
        <h2 id="panel-title">🚀 ساخت اتاق جدید</h2>
        <input type="text" id="inp-user" placeholder="نام شما (حداقل ۳ حرف)" maxlength="12" autocomplete="off">
        <button id="btn-action" class="btn-green" onclick="doAction()">ورود به بازی</button>
        <a href="/donate"><button>از ما حمایت کنید</button></a>
        <p class="note">بدون نیاز به ثبت نام • فقط نام خود را وارد کنید</p>
    </div>

    <!-- LOBBY PANEL -->
    <div id="lobby-panel" class="panel hidden">
        <h3> در انتظار بازیکنان...</h3>
        <div class="player-list" id="player-list"></div>
        <p style="font-size:0.85rem; color:#aaa;">لینک زیر را برای دوستانت بفرست:</p>
        <div id="share-box" onclick="copyLink()" title="برای کپی کلیک کنید">...</div>
        <p style="font-size:0.8rem; color:#888; margin-top:10px;">بازی با ورود نفر دوم خودکار شروع می‌شود</p>
    </div>

    <!-- Game UI -->
    <div id="overlay"></div>
    <div id="hud" class="hidden"></div>
    <canvas id="cvs" width="800" height="600"></canvas>

<script>
const API = window.location.origin;
let ws, user, roomID;
let state = { me: {x:400, y:500}, others: {}, bullets: [], enemies: [], scores: {} };
let cvs, ctx;
const keys = {};
let gameStarted = false;

// Detect if joining via link
const params = new URLSearchParams(window.location.search);
const isJoining = params.has('room');

if (isJoining) {
    document.getElementById('panel-title').innerText = "🎮 پیوستن به بازی";
    document.getElementById('btn-action').innerText = "ورود به اتاق";
    document.getElementById('btn-action').classList.remove('btn-green');
    document.getElementById('btn-action').classList.add('btn-blue');
    roomID = params.get('room');
}

async function doAction() {
    const u = document.getElementById('inp-user').value.trim();
    if(u.length < 3) return alert("نام باید حداقل  حرف باشد");
    
    const btn = document.getElementById('btn-action');
    btn.disabled = true;
    btn.innerText = "⏳ در حال اتصال...";
    user = u;

    try {
        if (isJoining) {
            // JOIN FLOW
            const r = await fetch(`${API}/join-room/${roomID}?username=${encodeURIComponent(user)}`);
            if(!r.ok) {
                const err = await r.json();
                throw new Error(err.detail);
            }
            // For joiners, go directly to game UI (they skip lobby visual)
            initGameUI();
            connectWS();
        } else {
            // HOST FLOW
            const cr = await fetch(`${API}/create-room`, {
                method:'POST', 
                headers:{'Content-Type':'application/json'}, 
                body: JSON.stringify({username: user})
            });
            if(!cr.ok) throw new Error("خطا در ساخت اتاق");
            
            const cd = await cr.json();
            roomID = cd.room_id;
            
            // SHOW LOBBY
            document.getElementById('auth-panel').classList.add('hidden');
            document.getElementById('lobby-panel').classList.remove('hidden');
            
            const link = `${window.location.origin}?room=${roomID}`;
            document.getElementById('share-box').innerText = link;
            updatePlayerList([user]);
            
            // Prepare game UI in background
            initGameUI();
            connectWS();
        }
    } catch(e) {
        alert("❌ خطا: " + e.message);
        btn.disabled = false;
        btn.innerText = isJoining ? "ورود به اتاق" : "ورود به بازی";
    }
}

function updatePlayerList(players) {
    const list = document.getElementById('player-list');
    list.innerHTML = players.map(p => 
        `<div class="player-item">
            <span>${p}</span>
            ${p === user ? '<span class="you-tag">شما</span>' : ''}
        </div>`
    ).join('');
}
function copyLink() {
    navigator.clipboard.writeText(document.getElementById('share-box').innerText)
        .then(() => {
            const box = document.getElementById('share-box');
            const orig = box.innerText;
            box.innerText = "✅ کپی شد!";
            setTimeout(() => box.innerText = orig, 1500);
        });
}

function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/game/${roomID}/${encodeURIComponent(user)}`;
    
    ws = new WebSocket(url);
    ws.onopen = () => console.log("Connected!");
    ws.onmessage = (e) => handle(JSON.parse(e.data));
    ws.onclose = (e) => {
        if(e.code !== 1000 && e.code !== 1001) {
            setTimeout(() => alert("اتصال قطع شد: " + (e.reason || "خطای ناشناخته")), 100);
        }
    };
}

function handle(m) {
    if(m.type === 'player_joined') {
        document.getElementById('p-count')?.remove(); // cleanup if exists
        updatePlayerList(m.players);
    }
    else if(m.type === 'countdown') {
        const ov = document.getElementById('overlay');
        ov.style.display = 'flex'; ov.innerText = m.value;
        if(m.value === 1) {
            setTimeout(() => {
                if(!gameStarted) {
                    ov.style.display='none';
                    startRender();
                }
            }, 1000);
        }
    }
    else if(m.type === 'game_start') {
        if(!gameStarted) {
            document.getElementById('overlay').style.display='none';
            startRender();
        }
    }
    else if(m.type === 'update_pos' && m.username !== user) state.others[m.username] = {x: m.x, y: m.y};
    else if(m.type === 'new_bullet') state.bullets.push({x: m.x, y: m.y, owner: m.username, vy: -8});
    else if(m.type === 'spawn_enemy') state.enemies.push({id: m.id, x: m.x, y: m.y, hp: 1});
    else if(m.type === 'score_update') { state.scores = m.scores; updateHUD(); }
    else if(m.type === 'game_over') { alert(`پایان بازی! برنده: ${m.winner}`); location.reload(); }
}

function updateHUD() {
    let h = "";
    for(let [u,s] of Object.entries(state.scores)) 
        h += `<div class="score-card" style="color:${u===user?'#2ecc71':'white'}">${u}: ${s}</div>`;
    document.getElementById('hud').innerHTML = h;
}

function initGameUI() {
    cvs = document.getElementById('cvs');
    ctx = cvs.getContext('2d');
    
    document.addEventListener('keydown', e => {
        keys[e.key] = true;
        if(e.key === ' ' && !e.repeat) shoot();
    });
    document.addEventListener('keyup', e => { keys[e.key] = false; });
}

function shoot() { 
    if(ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({type:'shoot', x:state.me.x, y:state.me.y})); 
    }
}

function startRender() {
    if(gameStarted) return;
    gameStarted = true;
    // Hide all panels when game starts
    document.getElementById('auth-panel').classList.add('hidden');
    document.getElementById('lobby-panel').classList.add('hidden');
    cvs.style.display = 'block';
    document.getElementById('hud').classList.remove('hidden');
    requestAnimationFrame(loop);
}

function loop() {
    if(!ctx || !gameStarted) return;
    
    const speed = 6;
    let moved = false;
    if(keys['ArrowLeft']) { state.me.x -= speed; moved = true; }
    if(keys['ArrowRight']) { state.me.x += speed; moved = true; }
    state.me.x = Math.max(20, Math.min(780, state.me.x));
    
    if(moved && ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({type:'move', x:state.me.x, y:state.me.y}));
    }
    
    ctx.clearRect(0,0,800,600);
    
    ctx.fillStyle = '#2ecc71';
    ctx.beginPath(); ctx.moveTo(state.me.x, state.me.y-15); ctx.lineTo(state.me.x-10, state.me.y+10); ctx.lineTo(state.me.x+10, state.me.y+10); ctx.fill();
    
    ctx.fillStyle = '#3498db';
    for(let [u, p] of Object.entries(state.others)) {
        ctx.beginPath(); ctx.moveTo(p.x, p.y-15); ctx.lineTo(p.x-10, p.y+10); ctx.lineTo(p.x+10, p.y+10); ctx.fill();
        ctx.fillStyle='white'; ctx.font='10px Tahoma'; ctx.fillText(u, p.x-10, p.y+25); ctx.fillStyle='#3498db';
    }
    
    ctx.fillStyle = '#f1c40f';
    state.bullets.forEach(b => { b.y += b.vy; ctx.fillRect(b.x-2, b.y, 4, 8); });
    
    ctx.fillStyle = '#e74c3c';
    state.enemies.forEach(e => {
        e.y += 2;
        ctx.beginPath(); ctx.arc(e.x, e.y, 12, 0, Math.PI*2); ctx.fill();
        if(e.hp > 0) {
            state.bullets.forEach(b => {
                if(b.owner === user && Math.abs(b.x-e.x)<15 && Math.abs(b.y-e.y)<15) {
                    e.hp = 0; b.y = -100;
                    ws.send(JSON.stringify({type:'enemy_killed'}));
                }
            });
        }
    });
    
    state.bullets = state.bullets.filter(b => b.y > -10);
    state.enemies = state.enemies.filter(e => e.hp > 0 && e.y < 620);
    requestAnimationFrame(loop);
}
</script>
</body>
</html>
"""
@app.get("/donate")
async def root():
    return HTMLResponse(GAME_DONATE_HTML)

GAME_DONATE_HTML = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>Space Shooter Lobby</title>
    <style>
        body { margin: 0; background: #0a0a12; color: white; font-family: Tahoma, sans-serif; overflow: hidden; display: flex; justify-content: center; align-items: center; height: 100vh; }
        canvas { border: 2px solid #333; background: #000; display: none; box-shadow: 0 0 30px rgba(0,255,0,0.1); }
        .panel { text-align: center; background: rgba(20,20,30,0.95); padding: 2rem; border-radius: 15px; border: 1px solid #444; width: 500px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); animation: fadeIn 0.3s ease; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        input { width: 85%; padding: 12px; margin: 8px 0; border-radius: 6px; border: 1px solid #555; background: #1a1a24; color: white; direction: ltr; text-align: center; font-size: 1.1rem;}
        button { width: 95%; padding: 12px; margin-top: 10px; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 1rem; transition: 0.2s; }
        button:disabled { opacity: 0.6; cursor: wait; filter: grayscale(0.8); }
        .btn-green { background: #2ecc71; color: white; } .btn-green:hover:not(:disabled) { background: #27ae60; }
        .btn-blue { background: #3498db; color: white; } .btn-blue:hover:not(:disabled) { background: #2980b9; }
        .hidden { display: none !important; }
        #share-box { background: #000; padding: 10px; border: 1px dashed #666; margin: 15px 0; word-break: break-all; font-size: 0.85rem; direction: ltr; color: #aaa; border-radius: 4px; cursor: pointer; }
        #share-box:hover { color: #fff; border-color: #888; }
        #overlay { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; justify-content: center; align-items: center; font-size: 8rem; font-weight: bold; color: white; text-shadow: 0 0 20px black; pointer-events: none; z-index: 100; }
        #hud { position: absolute; top: 10px; left: 10px; right: 10px; display: flex; justify-content: space-between; pointer-events: none; direction: ltr; }
        .score-card { background: rgba(0,0,0,0.6); padding: 5px 12px; border-radius: 4px; margin-right: 5px; border: 1px solid #333; }
        h2 { margin-top: 0; color: #2ecc71; }
        .note { font-size: 0.75rem; color: #888; margin-top: 15px; line-height: 1.4; }
        .player-list { text-align: right; margin: 15px 0; background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; }
        .player-item { padding: 5px 0; border-bottom: 1px solid #333; display: flex; justify-content: space-between; }
        .player-item:last-child { border-bottom: none; }
        .you-tag { color: #2ecc71; font-size: 0.8rem; background: rgba(46,204,113,0.1); padding: 2px 6px; border-radius: 4px; }
    </style>
</head>
<body>

    <!-- Login Panel -->
    <div id="auth-panel" class="panel">
        <h2 id="panel-title">حمایت مالی</h2>
        <p>
            bitcoin = bc1qvzqeadg9v7aa06gst3mgfjmc9wzvyvvpfj7s7v
            <br>
            satoshi = bc1qq8vaan43ns6mu7y8pcsn4m34yrk8ct0dd6dvhu
        </p>
        <a href="https://coffeebede.com/mahdiprogrammer"><button class="btn-green">دونیت ریالی</button></a>
        <a href="/"><button>بازگشت</button></a>
    </div>
    <!-- Game UI -->
    <div id="overlay"></div>
    <div id="hud" class="hidden"></div>
    <canvas id="cvs" width="800" height="600"></canvas>
"""
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)