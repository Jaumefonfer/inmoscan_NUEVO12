// Configuración del autosave
const AUTOSAVE_CONFIG = {
    interval: 5000, // 5 segundos
    storageKey: 'inmoScanData'
};

// Función para guardar datos en localStorage
function saveToLocalStorage(data) {
    try {
        localStorage.setItem(AUTOSAVE_CONFIG.storageKey, JSON.stringify(data));
        console.log('Datos guardados automáticamente');
    } catch (error) {
        console.error('Error al guardar datos:', error);
    }
}

// Función para cargar datos desde localStorage
function loadFromLocalStorage() {
    try {
        const data = localStorage.getItem(AUTOSAVE_CONFIG.storageKey);
        return data ? JSON.parse(data) : null;
    } catch (error) {
        console.error('Error al cargar datos:', error);
        return null;
    }
}

// Función para iniciar el autosave
function initAutosave() {
    let autosaveTimer;

    // Guardar datos cuando se detecten cambios
    function triggerAutosave() {
        if (autosaveTimer) {
            clearTimeout(autosaveTimer);
        }
        autosaveTimer = setTimeout(() => {
            const dataToSave = {
                // Aquí agregar los datos que quieres guardar
                // Por ejemplo:
                // searchQuery: document.getElementById('searchInput').value,
                // lastSearch: new Date().toISOString()
            };
            saveToLocalStorage(dataToSave);
        }, AUTOSAVE_CONFIG.interval);
    }

    // Agregar listeners para detectar cambios
    document.addEventListener('input', triggerAutosave);
    document.addEventListener('change', triggerAutosave);
    
    // Cargar datos al inicio
    const savedData = loadFromLocalStorage();
    if (savedData) {
        // Aquí restaurar los datos guardados
        // Por ejemplo:
        // document.getElementById('searchInput').value = savedData.searchQuery;
    }
}

// Iniciar autosave cuando la página cargue
window.addEventListener('DOMContentLoaded', initAutosave);
