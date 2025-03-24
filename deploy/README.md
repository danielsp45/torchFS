# Deployment Guide

## VM Setup

Create a `.env` file in the parent directory following the [.env.example](../.env.example) structure.

You need to specify your virtual machine provider and the VM ssh key path.

## Running the Environment

- Run the VM:

```bash
vagrant up
```

- Install Docker inside the VM

```bash
ansible-playbook install_docker.yml
```
