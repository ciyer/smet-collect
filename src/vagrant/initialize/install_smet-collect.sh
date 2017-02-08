# Install some basic tools
sudo apt-get -y update
sudo apt-get -y install git
sudo apt-get -y install unzip

# Install python3 (better utf-8 handling, but smet should work in python2 as well)
sudo apt-get -y install python3
sudo apt-get -y install python3-pip

# Install ruby -- some of the tweet processing happens in ruby
sudo apt-get -y install ruby

# Install jq -- used for tweet processing
sudo apt-get -y install jq

# Install the smet-collect package.
pip3 install -e /vagrant_src/python/smet-collect/
