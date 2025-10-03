$(document).on("click", ".btn-custom", function(event) {
    var target = event.currentTarget;
    if (!target) {
        return;
    }

    var tagName = target.tagName ? target.tagName.toLowerCase() : "";
    if (tagName !== "a") {
        return;
    }

    var href = target.getAttribute("href") || "";
    if (!href || href.trim() === "#" || href.trim().toLowerCase().startsWith("javascript:")) {
        event.preventDefault();
    }
});
