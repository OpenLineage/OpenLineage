// Fallback for gtag if Google Analytics script is blocked or not loaded
window.dataLayer = window.dataLayer || [];
function gtag() {
  dataLayer.push(arguments);
}
if (typeof window.gtag === "undefined") {
  window.gtag = gtag;
}
