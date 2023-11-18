if (window.location.hash) {
  document.getElementById(window.location.hash).scrollTo();
} else {
  scrollBy({ top: 99999999 });
}
