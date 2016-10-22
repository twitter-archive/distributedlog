# Apache DistributedLog (incubating) website

This is the website for [Apache DistributedLog](http://distributedlog.incubator.apache.org)
(incubating).

### About this site
The DistributedLog website is built using [Jekyll](http://jekyllrb.com/). Additionally,
for additional formatting capabilities, this website uses
[Twitter Bootstrap](http://getbootstrap.com/).

This website is hosted at:

    http://distributedlog.incubator.apache.org

It is important to note there are two sets of "website code"  with respect to
the Apache DistributedLog website.

1. The **Jekyll code** which contains all of the resources for building,
testing, styling, and tinkering with the website. Think of it as a website SDK.
1. The **static website** content which contains the content for the
website. This is the static content is what is actually hosted on the Apache 
DistributedLog website.

### Development setup

First, make sure you have Ruby installed. The minimal ruby version is 2.0.0.

Before working with the Jekyll code, you can use Ruby's Bundler Gem to install all the dependencies for building this website:

    gem install bundler
    bundle install

*If you are on a Mac, you may need to install
[Ruby Gems](https://rubygems.org/pages/download).*

And use [pip](https://pypi.python.org/pypi/pip) to install docutils and pygments required for building the documentation:

    pip install docutils pygments

### Build website together with documentation

You can run the `build.sh` at the root folder of `website` to build a local site.

    $ build.sh local

You can also build with the production settings. It will generate the content that is actually hosted in distributedlog website.

    $ build.sh production

Once built, it will be placed in the folder `content` inside of the root directory.
This directory will include images, HTML, CSS and so on. In a typical Jekyll install
this content will live in `_site` - it has been changed for Apache DistributedLog website
to work with the ASF Incubator publishing system.

### Run the server locally

You can run `bundle exec jekyll serve` locally to serve the generated static content to verify if the website works as expected.

    $ bundle exec jekyll serve


