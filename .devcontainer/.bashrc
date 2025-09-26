
# If not running interactively, don't do anything
case $- in
    *i*) ;;
      *) return;;
esac

# basic history control
HISTCONTROL=ignoreboth
shopt -s histappend
HISTSIZE=1000
HISTFILESIZE=2000

# check the window size after each command and, if necessary,
# update the values of LINES and COLUMNS.
shopt -s checkwinsize

color_prompt=yes
PS1='\[\033[01;32m\]devcontainer\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ '


if ! docker ps > /dev/null 2>&1; then
    nohup dockerd > /var/log/dockerd.log 2>&1 &
fi
