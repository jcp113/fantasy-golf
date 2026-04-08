const API = '';  // same origin

async function apiFetch(path) {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const MAJOR_WEEKS = [10, 15, 20, 24];

function isMajor(weekNum) {
  return MAJOR_WEEKS.includes(weekNum);
}

function formatMoney(amount) {
  if (!amount) return '$0';
  return '$' + Number(amount).toLocaleString();
}

function showLoading(containerId) {
  const el = document.getElementById(containerId);
  if (el) {
    el.innerHTML = `
      <div class="loading">
        <div class="loading-spinner"></div>
        <div>Loading data...</div>
      </div>
    `;
  }
}

function showError(containerId, message) {
  const el = document.getElementById(containerId);
  if (el) {
    el.innerHTML = `<div class="error-message">${message}</div>`;
  }
}
