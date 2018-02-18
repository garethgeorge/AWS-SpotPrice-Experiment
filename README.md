# Instructions

## Dependencies

```
pip install boto3 pyyaml
```

## Setup 
You will need your private key (or the private key of the server running the experiment) added to 169.231.235.184 so that you can shell in and get the latest data!
```
git submodule init
git submodule update -r 
sh build-sip-processor.sh 
```
Now run experiment.py

## Email with Rich

Server location
```
ssh root@169.231.235.184

private key: gareth@lastpengu.in.pub and gareth.lastpengu.in
```

## Results for my account 
```
Resulting mappings:
  us-east-1a - (563, 'us-east-1a')
  us-east-1b - (705, 'us-east-1b')
  us-east-1c - (738, 'us-east-1c')
  us-east-1d - (850, 'us-east-1d')
  us-east-1e - (629, 'us-east-1e')
  us-east-1f - (442, 'us-east-1f')
  us-west-2a - (928, 'us-west-2a')
  us-west-2b - (914, 'us-west-2b')
  us-west-2c - (891, 'us-west-2c')
  
  I suspect there is something pathological going on with the following two
  us-west-1a - (0, 'us-west-1a')
  us-west-1c - (1412, 'us-west-1c')

  I spoke with @William Berman and he indicated that the types of wonky mappings I am seeing are perhaps expected... he suspects that AWS does not actually obfuscate the AZ names though it reserves the right to do so.

  It may be worth actually contacting amazon to confirm that this is indeed how things work.
```

## Candidate instance types
```
us-east-1a r4.large 

```