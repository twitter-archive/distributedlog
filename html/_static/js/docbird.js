function makeDraftDocument() {
  var path = window.location.pathname;
  if (path.startsWith('/docbird/staging')) {
    var projectHeaders = $('.db-header-projectname h1');
    projectHeaders.append('  <span>(draft)</span>');

    $('#rateYo').hide();

    $.ajax({
      type: "GET",
      url: path,
      success: function(res, status, xhr) {
        var projectTitle = $('.db-project-info .db-code-link');
        var expires = xhr.getResponseHeader('x-ton-expires');
        var draftAdmonition = '<div class="admonition important alert alert-info">' +
          '<p class="last">The document that you\'re viewing is a draft. ' +
          'It expires on ' + expires + '.</p>' +
          '</div>';
        projectTitle.after(draftAdmonition);
      }
    });
  }
}

$(function() {
  makeDraftDocument();

  // Adjust header height on scroll
  var showing = true;
  var scroll_handler = function() {
    var pos = $(window).scrollTop();
    var large_doc = $(window).height() + 180 < $(document).height();
    // Only hide the header for longer pages else header gets small, scroll pos drops to zero, header gets big...
    if (showing && pos > 60 && large_doc) {
      // hide the header
      showing = false;
      $("body").addClass('db-header-small');
      if (pos > 150) {
        $(window).scrollTop(pos - 75);
      } else {
        $(window).scrollTop(75);
      }
    }

    if (!showing && pos < 60) {
      showing = true;
      $("body").removeClass('db-header-small');
      $(window).scrollTop(0);
    }
  };
  $(window).scroll(scroll_handler);
});

$(function() {
  $('.graphviz').each(function() {
    var $img = $(this).find("img:last");
    $(this).prepend($("<img class='mag' src='_static/mag.png'>"));
    $(this).wrapInner($('<a>', {'href': $img.attr("src"), 'data-featherlight': 'image'}));
  });
  $.featherlight();
});
