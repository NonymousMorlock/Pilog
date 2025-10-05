// Relatively new way to do it, works in Chrome, Edge, and Opera
// https://developer.mozilla.org/en-US/docs/Web/API/Window/showDirectoryPicker

async function pickDirectory() {
  try {
    const directoryHandle = await window.showDirectoryPicker();
    const inputField = document.getElementById("folderPicker")
    inputField.value = directoryHandle.name;
    inputField.dataset.path = directoryHandle.name; // Store the directory name in a data attribute
    console.log("Selected directory:", directoryHandle.name);
  } catch (error) {
    console.error("Error picking directory:", error);
  }
}