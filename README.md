# smet-collect

Underlying [SMET](http://smet.li) are tools built in python. These tools are designed to be efficient, but also support reproducible research by explicitly capturing what was done when.

SMET searches twitter at periodic intervals and stores results both in original form, as returned by the twitter API, and in a data-reduced form, capturing only the fields that are of interest. This makes it possible to work daily with only the data that is necessary for analysis, while making it possible to go back to the original source if that is ever necessary.

The central tool of SMET is the python package smetcollect, which has functionality for collecting data from twitter.

# Quickstart

The easiest way to try smet-collect out is to use vagrant. In preparation, you need to enter your twitter API credentials into the `credentials.yaml` files in `examples/2017_france` and `examples/2017_us_congress_115`. In each folder there is a template, `credentials-template.yaml` -- enter your credentials there and rename the file to `credentials.yaml`.

Then, in the src/vargrant folder

    vagrant up
    vagrant ssh
    smet-collect pipeline /vagrant_examples/2017_us_congress_115
    smet-collect pipeline /vagrant_examples/2017_france    
    

Once that completes, you will find results in the folders `examples/2017_us_congress_115` and `examples/2017_france`. The immediately most interesting is the subfolder `pruned`. Each configured race will have twitter data in its subfolder. The pruned data are in a straightforward to understand json format.

# Installation

## Requirements

smet-collect requires that python, ruby, and [jq](https://stedolan.github.io/jq/) are available in the PATH. The file `src/vagrant/initialize/install_smet-collect.sh` explicitly lists all the prerequisites. Once they are installed, you can install smet-collect using pip, e.g., 

    pip install -e src/python/smet-collect/
    

# Usage

When the smetcollect package is installed (e.g., using pip), it also installs the smet command-line client. The command-line client operates on a folder, called a `bundle`, which conforms to a particular structure (described below).

    Usage: smet-collect [OPTIONS] COMMAND [ARGS]...

    Options:
    -q, --quiet  Suppress status reporting.
    --help       Show this message and exit.

    Commands:
    archive        Delete the redundant data for runs that have been compressed.
    collect        Collect data for a bundle.
    compress       Compress pruned runs in a bundle.
    pipeline       Collect data, prune it, compress it, and delete the raw, uncompressed data.
    prune          Prune down bundle run data to the relevant...
    rebuild        Rebuild prune data in a bundle.
    uncompress     Uncompress runs in a bundle.

The `pipeline` command covers the standard usage pattern which is:

- Collect data from twitter
- Prune data to the relevant fields
- Compress the full responses from twitter
- Delete the (uncompressed) raw data, leaving only the pruned and compressed (raw) data.

You will probably want to put this into the cron to run at regular intervals:

    smet-collect pipeline ~/collect/2016-us-pres-primary

# Bundle Structure

A bundle is a folder that, initially, contains two files.

- config.yaml
- credentials.yaml

After data is collected, it grows to the following structure.

    config.yaml        [configuration file for searches]
    credentials.yaml   [configuration file for twitter API]
    compressed/        [parent folder for compressed data]
    pruned/            [parent folder for pruned data]
    raw/               [parent for raw data]

## credentials.yaml

This file contains the credentials for the twitter api. It has two keys.

    app_key: [twitter app key]
    access_token: [twitter access token]

## config.yaml

The config file defines what searches should be run.

    - candidates:
      - name: Donald Trump
        party: Republican
        search: [Trump, "@realDonaldTrump"]
      - name: Jeb Bush
        party: Republican
        search: [Jeb Bush, "@JebBush"]
      - name: Ted Cruz
        party: Republican
        search: [Ted Cruz, "@tedcruz"]
      - name: Marco Rubio
        party: Republican
        search: [Marco Rubio, "@marcorubio"]
        active: No
      - name: John Kasich
        party: Republican
        search: [John Kasich, "@JohnKasich"]
      race: 2016 National Republican Primary
      year: 2016
    - candidates:
      - name: Hillary Clinton
        party: Democratic
        search: [Hillary, Clinton, "@HillaryClinton"]
      - name: Bernie Sanders
        party: Democratic
        search: [Bernie, Sanders, "@SenSanders", "@BernieSanders"]
      - name: Martin O'Malley
        party: Democratic
        search: [O'Malley, "@MartinOMalley"]
      race: 2016 National Democratic Primary
      year: 2016

N.b. #hashtags and @user mentions need to be quoted, otherwise the YAML file is not valid.


The structure of the file is somewhat specific to tracking elections, but the functionality is rather generic, though you may need to ignore some of the fields.

Each time smet-collect collect or smet-collect pipeline is called, it creates a collection run. A run groups together search results for each term in each candidate in each race and is identified by a timestamp (the time the collection run was started). For each term, the query to twitter asks only for tweets newer than the newest tweet from the previous run.

Smet imposes an implicit hierarchical structure on searches to twitter: race/run/candidate_name/term, where a race can be composed of multiple candidates, and a candidate can be associated with multiple terms. This structure can either be exploited or (partially) ignored in analysis.

## Output

As smet-collect is used, the bundle grows to contain additional subfolders:

- compressed -- parent for compressed data
- pruned -- parent for pruned data
- raw -- parent for raw data

Example:

Data for the run on March 3, 2016 at 5 AM (GMT) is stored in several locations:

- 2016-us-pres-primary/compressed/2016-national-democratic-primary/2016-03-21-05-01-02-440085_run.tar.bz2
- 2016-us-pres-primary/pruned/2016-national-democratic-primary/2016-03-21-05-01-02-440085_run.json
- 2016-us-pres-primary/compressed/2016-national-democratic-primary/2016-03-21-05-01-02-440085/[multiple json files -- one for each response from the twitter API]

