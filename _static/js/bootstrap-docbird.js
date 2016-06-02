(function ($) {
  /**
   * Patch all tables to remove ``docutils`` class and add Bootstrap base
   * ``table`` class.
   */
  patchTables = function () {
    $("table.docutils")
      .removeClass("docutils")
      .addClass("table")
      .attr("border", 0);
  };

  $(window).load(function () {
    /*
     * Scroll the window to avoid the topnav bar
     * https://github.com/twitter/bootstrap/issues/1768
     */
    if ($("#navbar.navbar-fixed-top").length > 0) {
      var navHeight = $("#navbar").height(),
        shiftWindow = function() { scrollBy(0, -navHeight - 10); };

      if (location.hash) {
        setTimeout(shiftWindow, 1);
      }

      window.addEventListener("hashchange", shiftWindow);
    }
  });

  $(document).ready(function () {
    // Add styling, structure to TOC's.
    $(".dropdown-menu").each(function () {
      $(this).find("ul").each(function (index, item){
        var $item = $(item);
        $item.addClass('unstyled');
      });
    });

    // Patch tables.
    patchTables();

    // Add Note, Warning styles. (BS v2,3 compatible).
    $('.admonition').addClass('alert alert-info')
      .filter('.warning, .caution')
        .removeClass('alert-info')
        .addClass('alert-warning').end()
      .filter('.error, .danger')
        .removeClass('alert-info')
        .addClass('alert-danger alert-error').end();

    // Inline code styles to Bootstrap style.
    $('tt.docutils.literal').not(".xref").each(function (i, e) {
      // ignore references
      if (!$(e).parent().hasClass("reference")) {
        $(e).replaceWith(function () {
          return $("<code />").html($(this).html());
        });
      }});

    // Update sourcelink to remove outerdiv (fixes appearance in navbar).
    var $srcLink = $(".nav #sourcelink");
    $srcLink.parent().html($srcLink.html());
  });
}(window.$jqTheme || window.jQuery));
