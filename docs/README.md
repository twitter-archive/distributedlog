This README gives an overview of how to build and contribute to the documentation of Apache DistributedLog.

The documentation is included with the source of Apache DistributedLog in order to ensure that you always
have docs corresponding to your checked out version. The online documentation at
http://distributedlog.incubator.apache.org/docs/latest is also generated from the files found at master branch.

# Requirements

First, make sure you have Ruby installed. The minimal ruby version is 2.0.0.

The dependencies are declared in the Gemfile in this directory. We use [reStructuredText](http://docutils.sourceforge.net/rst.html)
to write and [Jekyll](https://jekyllrb.com/) and [jekyll-rst](https://github.com/xdissent/jekyll-rst) to translate the documentation to static HTML.
You can use Ruby's Bundler Gem to install all the depenencies for building the documentation:

    gem install bundler
    bundle install

And use [pip](https://pypi.python.org/pypi/pip) to install docutils and pygments

    pip install docutils pygments

# Build

## Live developement

While you are working with the documentation, you can test and develop live. Run the following in the root folder of the `docs`:

    $ bundle exec jekyll serve

Jekyll will start a webserver on port `4000`. As you make changes to the content, Jekyll will rebuild it automatically.
This is helpful if you want to see how your changes will render in realtime.

## Generating the static website

Once you are done with your changes, you need to compile the static content for the website.
This is what is actually hosted on the Apache DistributedLog documentation site.

You can build the static content by running the following command in the root docs directory:

    $ jekyll build

Once built, it will be placed in the folder `_site` inside of the root directory. This directory will include images, HTML, CSS, and so on.

# Contribute

## reStructuredText

The documentation pages are written in [reStructuredText](http://docutils.sourceforge.net/rst.html). It is possible to use [Markdown](http://daringfireball.net/projects/markdown/syntax) and intermix plain html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the page and the layout to use.
The title is used as the top-level heading for the page.

There are two layouts (found in `_layouts`) used for writing documentation: one is `default`, while the other one is `guide`.
The pages that use `default` layout will not have navigation side bar, while the pages that use `guide` layout will have
navigation side bar. The pages under `user-guide` and `admin-guide` are written using `guide` layout.

    ---
    title: "Title of the Page"
    layout: default 
    ---

Furthermore, you can access variables found in `docs/_config.yml` as follows:

    {{ site.NAME }}

This will be replaced with the value of the variable called `NAME` when generating the docs.

## Structure

### Documentation

#### Navigation

The navigation on the left side of the docs is automatically generated when building the docs. You can modify the markup in `_layouts/guide.html`.

The structure of the navigation is determined by the front matter of all pages. The fields used to determine the structure are:

- `nav-id` => ID of this page. Other pages can use this ID as their parent ID.
- `nav-parent_id` => ID of the parent. This page will be listed under the page with id `nav-parent_id`.

Level 0 is made up of all pages, which have nav-parent_id set to `_root_`. There is no limitation on how many levels you can nest.

The `title` of the page is used as the default link text. You can override this via `nav-title`. The relative position per navigational level is determined by `nav-pos`.

The nesting is also used for the breadcrumbs like `User Guide > API > Best Practise`.
