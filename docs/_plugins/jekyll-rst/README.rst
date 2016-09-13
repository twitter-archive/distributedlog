Overview
========

This plugin adds `ReStructuredText`_ support to `Jekyll`_ and `Octopress`_. 
It renders ReST in posts and pages, and provides a custom directive to
support Octopress-compatible syntax highlighting.

Requirements
============

* Jekyll *or* Octopress >= 2.0
* Docutils
* Pygments
* `RbST`_

Installation
============

1. Install Docutils and Pygments. 

   The most convenient way is to use virtualenv_burrito:

   ::

      $ curl -s https://raw.github.com/brainsik/virtualenv-burrito/master/virtualenv-burrito.sh | bash
      $ source /Users/xdissent/.venvburrito/startup.sh
      $ mkvirtualenv jekyll-rst
      $ pip install docutils pygments

2. Install RbST.

   If you use `bundler`_ with Octopress, add ``gem 'RbST'`` to 
   your ``Gemfile`` in the ``development`` group, then run 
   ``bundle install``. Otherwise, ``gem install RbST``.

3. Install the plugin.

   For Jekyll:

   ::

      $ cd <jekyll-project-path>
      $ git submodule add https://github.com/xdissent/jekyll-rst.git _plugins/jekyll-rst

   For Octopress:

   ::

      $ cd <octopress-project-path>
      $ git submodule add https://github.com/xdissent/jekyll-rst.git plugins/jekyll-rst

4. Start blogging in ReStructuredText. Any file with the ``.rst`` extension
   will be parsed as ReST and rendered into HTML.

   .. note:: Be sure to activate the ``jekyll-rst`` virtualenv before generating
      the site by issuing a ``workon jekyll-rst``. I suggest you follow `Harry
      Marr's advice`_ and create a ``.venv`` file that will  automatically 
      activate the ``jekyll-rst`` virtualenv when you ``cd`` into your project.

Source Code Highlighting
========================

A ``code-block`` ReST directive is registered and aliased as ``sourcecode``. 
It adds syntax highlighting to code blocks in your documents::

   .. code-block:: ruby
      
      # Output "I love ReST"
      say = "I love ReST"
      puts say

Optional arguments exist to supply a caption, link, and link title::

   .. code-block:: console
      :caption: Read Hacker News on a budget
      :url: http://news.ycombinator.com
      :title: Hacker News

      $ curl http://news.ycombinator.com | less

Octopress already includes style sheets for syntax highlighting, but you'll
need to generate one yourself if using Jekyll::

   $ pygmentize -S default -f html > css/pygments.css

Octopress Tips
==============

* Use ``.. more`` in your ReST documents to indicate where Octopress's 
  ``excerpt`` tag should split your content for summary views.

.. _ReStructuredText: http://docutils.sourceforge.net/rst.html
.. _Jekyll: http://jekyllrb.com/
.. _Octopress: http://octopress.com/
.. _RbST: http://rubygems.org/gems/RbST
.. _bundler: http://gembundler.com/
.. _Harry Marr's advice: http://hmarr.com/2010/jan/19/making-virtualenv-play-nice-with-git/