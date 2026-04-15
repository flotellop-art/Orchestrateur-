const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  getServerUrl:  () => ipcRenderer.invoke('get-server-url'),
  getAppVersion: () => ipcRenderer.invoke('get-app-version'),
  quitApp:       () => ipcRenderer.send('quit'),
  showWindow:    () => ipcRenderer.send('show-window'),
  platform:      process.platform,
  // Sélecteur de dossier natif
  selectFolder: () => ipcRenderer.invoke('select-folder'),
  // Ouvrir une URL externe
  openExternal: (url) => ipcRenderer.invoke('open-external', url),
});
