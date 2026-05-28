const{app,BrowserWindow,Menu,Tray,shell,dialog,ipcMain}=require('electron');
const path=require('path');
const{spawn}=require('child_process');
const http=require('http');
const fs=require('fs');
const security=require('./electron_hardening');

const PORT=8000,BASE='http://127.0.0.1:8000',TITLE='Multi-Agent Orchestrator';
let win=null,splash=null,tray=null,srv=null,quitting=false,relaunching=false;

function appRoot(){return app.isPackaged?path.join(process.resourcesPath,'app'):path.join(__dirname,'..');}

function pyPath(){
  const r=appRoot();
  for(const p of[path.join(r,'venv','Scripts','python.exe'),path.join(r,'venv','bin','python'),path.join(r,'.venv','Scripts','python.exe'),path.join(r,'.venv','bin','python')]){
    try{if(fs.existsSync(p))return p;}catch(e){}
  }
  return 'python';
}

function checkServer(){
  return new Promise(res=>{
    const r=http.get(BASE+'/api/stats',resp=>{res(resp.statusCode===200);});
    r.on('error',()=>res(false));
    r.setTimeout(2000,()=>{r.destroy();res(false);});
  });
}

async function startServer(){
  if(await checkServer()){console.log('[srv] already running');return true;}
  const root=appRoot(),py=pyPath(),script=path.join(root,'orchestrator.py');
  console.log('[srv] starting',py,script,'cwd:',root);
  srv=spawn(py,[script],{cwd:root,env:{...process.env},stdio:['ignore','pipe','pipe']});
  srv.stdout.on('data',d=>console.log('[py]',d.toString().trim()));
  srv.stderr.on('data',d=>console.warn('[py!]',d.toString().trim()));
  srv.on('close',code=>{srv=null;if(!quitting)dialog.showErrorBox('Serveur arrete','Code: '+code);});
  return new Promise(res=>{
    const t=Date.now();
    const iv=setInterval(async()=>{
      if(await checkServer()){clearInterval(iv);console.log('[srv] ready');res(true);}
      else if(Date.now()-t>30000){clearInterval(iv);console.error('[srv] timeout');res(false);}
    },500);
  });
}

function stopServer(){if(srv){srv.kill('SIGTERM');srv=null;}}

function createSplash(){
  splash=new BrowserWindow({width:460,height:280,frame:false,alwaysOnTop:true,resizable:false,center:true,backgroundColor:'#2b2b2b',webPreferences:{nodeIntegration:false,contextIsolation:true,sandbox:true}});
  splash.loadURL('data:text/html;charset=utf-8,'+encodeURIComponent('<!DOCTYPE html><html><head><meta charset=UTF-8><style>*{margin:0;padding:0;box-sizing:border-box}body{background:#2b2b2b;color:#fff;font-family:-apple-system,sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;gap:20px;-webkit-app-region:drag}h1{font-size:1.5rem}h1 span{color:#E8825A}.ld{width:36px;height:36px;border:3px solid #444;border-top-color:#E8825A;border-radius:50%;animation:s .8s linear infinite}@keyframes s{to{transform:rotate(360deg)}}.st{color:#888;font-size:.8rem}</style></head><body><h1>Multi-Agent <span>Orchestrator</span></h1><div class=ld></div><p class=st>Demarrage du serveur...</p></body></html>'));
}

function buildMenu(){
  Menu.setApplicationMenu(Menu.buildFromTemplate([
    {label:TITLE,submenu:[{label:'A propos',role:'about'},{type:'separator'},{label:'Quitter',accelerator:'CmdOrCtrl+Q',click:()=>quit()}]},
    {label:'Vue',submenu:[{role:'reload',accelerator:'CmdOrCtrl+R'},{role:'zoomIn'},{role:'zoomOut'},{role:'resetZoom'},{type:'separator'},{role:'togglefullscreen',accelerator:'F11'},{type:'separator'},{role:'toggleDevTools',accelerator:'F12'}]},
    {label:'Navigation',submenu:[
      {label:'Tableau de bord',accelerator:'CmdOrCtrl+1',click:()=>win&&win.loadURL(BASE)},
      {label:'Chat',accelerator:'CmdOrCtrl+2',click:()=>win&&win.loadURL(BASE+'/chat')},
      {type:'separator'},
      {label:'Ouvrir dans navigateur',click:()=>security.safeOpenExternal(BASE)}
    ]},
    {label:'Aide',submenu:[{label:'Documentation',click:()=>security.safeOpenExternal('https://docs.anthropic.com')}]}
  ]));
}

function createWin(){
  win=new BrowserWindow({width:1280,height:800,minWidth:800,minHeight:600,show:false,title:TITLE,backgroundColor:'#f7f7f7',
    webPreferences:security.getSecureWebPreferences()});
  security.attachSecurityHooks(win);
  buildMenu();
  win.webContents.session.clearCache().then(()=>win.loadURL(BASE));
  win.once('ready-to-show',()=>{if(splash&&!splash.isDestroyed()){splash.close();splash=null;}win.show();win.focus();});
  win.on('close',e=>{if(!quitting){e.preventDefault();win.hide();}});
  win.on('closed',()=>{win=null;});
  win.webContents.on('did-fail-load',()=>{if(!quitting)win.loadURL('data:text/html,<body style="font-family:sans-serif;text-align:center;padding:40px"><h2>Connexion echouee</h2><br><button onclick="location.reload()">Reessayer</button></body>');});
}

function createTray(){
  const icon=path.join(__dirname,'assets',process.platform==='win32'?'icon.ico':'icon.png');
  try{
    tray=new Tray(icon);
    tray.setToolTip(TITLE);
    tray.setContextMenu(Menu.buildFromTemplate([
      {label:TITLE,enabled:false},{type:'separator'},
      {label:'Afficher',click:()=>show()},
      {label:'Tableau de bord',click:()=>{show();win&&win.webContents.session.clearCache().then(()=>win.loadURL(BASE));}},
      {label:'Chat',click:()=>{show();win&&win.loadURL(BASE+'/chat');}},
      {type:'separator'},{label:'Quitter',click:()=>quit()}
    ]));
    tray.on('double-click',()=>show());
  }catch(e){console.warn('[tray]',e.message);}
}

function show(){if(win){win.show();win.focus();if(win.isMinimized())win.restore();}}
function quit(){quitting=true;stopServer();app.quit();}


ipcMain.handle('select-folder', async () => {
  const result = await dialog.showOpenDialog(win || null, {
    properties: ['openDirectory', 'createDirectory'],
    title: 'Choisir le dossier du projet'
  });
  return result.canceled ? null : result.filePaths[0];
});

ipcMain.handle('open-external', (event, url) => { security.safeOpenExternal(url); });

ipcMain.handle('get-server-url',()=>BASE);
ipcMain.handle('get-version',()=>app.getVersion());
ipcMain.on('quit',()=>quit());

app.whenReady().then(async()=>{
  if(!app.requestSingleInstanceLock()){app.quit();return;}
  app.on('second-instance',()=>show());
  security.setupSecureSession();
  createSplash();
  const ok=await startServer();
  if(!ok){
    if(splash&&!splash.isDestroyed())splash.close();
    const c=dialog.showMessageBoxSync({type:'error',title:'Erreur',message:'Impossible de demarrer le serveur Python.',detail:'Python: '+pyPath()+'\nRacine: '+appRoot(),buttons:['Reessayer','Quitter']});
    if(c===0){relaunching=true;app.relaunch();}
    app.quit();return;
  }
  createWin();
  createTray();
});

app.on('activate',()=>{if(BrowserWindow.getAllWindows().length===0)createWin();else show();});
app.on('before-quit',()=>{quitting=true;if(!relaunching)stopServer();});
process.on('SIGTERM',()=>quit());
process.on('SIGINT',()=>quit());