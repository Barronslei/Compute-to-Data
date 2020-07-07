# Compute-to-Data
 provides a means to exchange data while preserving privacy.


# Setup logging

manta_logging.logger.setLevel('INFO')

print("squid-py Ocean API version:", squid_py.__version__)

# Get the configuration file path for this environment


CONFIG_PATH = Path(os.path.expanduser(os.environ['CONFIG_PATH']))

assert CONFIG_PATH.exists(), "{} - path does not exist".format(CONFIG_PATH)

logging.critical("Configuration file selected: {}".format(CONFIG_PATH))

logging.critical("Deployment type: {}".format(config.get_deployment_type()))

logging.critical("Squid API version: {}".format(squid_py.__version__))

# Instantiate test with the default configuration file.


configuration = Config(CONFIG_PATH)

ConfigProvider.set_config(configuration)

ocn = test(configuration)

# faucet is used to request eth from the TESTNET (e.g. the Nile testnet)

faucet_url = ocn.config.get('keeper-contracts', 'faucet.url')

scale = 10**18

# Prepare the provider address to use when creating the asset and service agreement

web3 = Web3Provider.get_web3()

provider_address = web3.toChecksumAddress(configuration.provider_address)

print(f'Will be using provider (Brizo) address: {provider_address}')

#We need accounts for the publisher and consumer, let's make new ones


# Publisher account (will be filled with some TESTNET eth automatically)

publisher_acct = create_account(faucet_url, wait=True)

print("Publisher account address: {}".format(publisher_acct.address))

time.sleep(5)  # wait a bit more for the eth transaction to validate

# ensure test token balance

if ocn.accounts.balance(publisher_acct).ocn/scale < 100:
    ocn.accounts.request_tokens(publisher_acct, 100)

print("Publisher account Testnet 'ETH' balance: {:>6.3f}".format(ocn.accounts.balance(publisher_acct).eth/scale))

print("Publisher account Testnet test balance: {:>6.3f}".format(ocn.accounts.balance(publisher_acct).ocn/scale))

assert ocn.accounts.balance(publisher_acct).eth/scale > 0.0, 'Cannot continue without eth.'

# Consumer account (same as above, will have some TESTNET eth added)

consumer_account = create_account(faucet_url, wait=True)

print("Consumer account address: {}".format(consumer_account.address))

time.sleep(5)  # wait a bit more for the eth transaction to validate

if ocn.accounts.balance(consumer_account).ocn/scale < 100:
    ocn.accounts.request_tokens(consumer_account, 100)
    
# Verify both test and eth balance

print("Consumer account Testnet 'ETH' balance: {:>6.3f}".format(ocn.accounts.balance(consumer_account).eth/scale))

print("Consumer account Testnet Ocean balance: {:>6.3f}".format(ocn.accounts.balance(consumer_account).ocn/scale))

assert ocn.accounts.balance(consumer_account).eth/scale > 0.0, 'Cannot continue without eth.'

assert ocn.accounts.balance(consumer_account).ocn/scale > 0.0, 'Cannot continue without Ocean Tokens.'

#Section 1: Publish the asset with compute service

# Build compute service to be included in the asset DDO.

cluster = ocn.compute.build_cluster_attributes('kubernetes', '/cluster/url')

containers = [ocn.compute.build_container_attributes(
    "tensorflow/tensorflow",
    "latest",
    "sha256:cb57ecfa6ebbefd8ffc7f75c0f00e57a7fa739578a429b6f72a0df19315deadc")
]

servers = [ocn.compute.build_server_attributes('1', 'xlsize', 16, 0, '16gb', '1tb', 2242244)]

provider_attributes = ocn.compute.build_service_provider_attributes(
    'Azure', 'Compute power 1', cluster, containers, servers
)

attributes = ocn.compute.create_compute_service_attributes(
    13, 3600, publisher_acct.address, get_timestamp(), provider_attributes
)

service_endpoint = Brizo.get_compute_endpoint(ocn.config)

template_id = ocn.keeper.template_manager.create_template_id(
    ocn.keeper.template_manager.SERVICE_TO_TEMPLATE_NAME['compute']
)

service_descriptor = ServiceDescriptor.compute_service_descriptor(attributes, service_endpoint, template_id)

# Get example of Meta Data from file

metadata = get_metadata_example()

# Print the entire (JSON) dictionary

# pprint(metadata)

# With this metadata object prepared, we are ready to publish the asset into Ocean Protocol.

ddo = ocn.assets.create(
    metadata,
    publisher_acct,
    [service_descriptor],
    providers=[provider_address],
    use_secret_store=False
)

assert ddo, 'asset registration failed.'

registered_did = ddo.did

print("New asset registered at", registered_did)

asset = ocn.assets.resolve(ddo.did)

assert asset and asset.did == ddo.did, 'Something is not right.'

#Let's take a look at the compute service from the published DDO

compute_service = ddo.get_service(ServiceTypes.CLOUD_COMPUTE)

pprint("Compute service definition: \n{}".format(json.dumps(compute_service.as_dictionary(), indent=2)))

#Section 2: Preparing the algorithm that will be run

Grab the algorithm example from mantaray_utilities

algorithm_text = get_algorithm_example()

print(f'algorithm: \n{algorithm_text}')

# build the algorithm metadata object to use in the compute request

algorithm_meta = AlgorithmMetadata(
    {
        'language': 'python',
        'rawcode': algorithm_text,
        'container': {
            'tag': 'latest',
            'image': 'amancevice/pandas',
            'entrypoint': 'python $ALGO'
        }
    }
)

# print(f'algorith meta: {algorithm_meta.as_dictionary()}')

#Section 3: Subscribe to the compute service

#Now we can prepare for running the remote compute, first we need to start an agreement to buy the service


# First step in buying a service is placing the order which creates

# and starts the service agreement process, consumer payment is

# processed automatically once the agreement is created successfully.

agreement_id = ocn.compute.order(
    ddo.did,
    consumer_account,
    provider_address=provider_address
)

print(f'Got agreementId: {agreement_id}')

event = ocn.keeper.agreement_manager.subscribe_agreement_created(agreement_id, 20, None, (), wait=True)

if event:
    print(f'Got agreement event {agreement_id}: {event}')

else:
    print(f'Cannot find agreement event, could be a VM transaction error.')


# Wait for payment transaction

payment_locked_event = ocn.keeper.lock_reward_condition.subscribe_condition_fulfilled(
    agreement_id, 30, None, [], wait=True, from_block=0
)

assert payment_locked_event, 'payment event was not found'

print('Payment was successful.')

# and wait for service agreement approval from the provider end

compute_approval_event = ocn.keeper.compute_execution_condition.subscribe_condition_fulfilled(
    agreement_id, 30, None, [], wait=True, from_block=0
)

assert compute_approval_event, 'compute agreement is not approved yet.'

print('Provider approval was successful.')

Section 4: Run the algorithm remotely on the dataset

Now that the agreement is approved by the provider, we can start the compute job


# Submit algorithm to start the compute job

job_id = ocn.compute.start(agreement_id, ddo.did, consumer_account, algorithm_meta=algorithm_meta)

print(f'compute job started: jobId={job_id}')

# check the compute job status

status = ocn.compute.status(agreement_id, job_id, consumer_account)

print(f'compute job status: {status}')

#Wait for results

trials = 0

result = ocn.compute.result(agreement_id, job_id, consumer_account)

while not result.get('urls'):
    print(f'result not available yet, trial {trials}/30, '
          f'status is: {ocn.compute.status(agreement_id, job_id, consumer_account)}')
    
    time.sleep(10)
    
    result = ocn.compute.result(agreement_id, job_id, consumer_account)
    
    trials = trials + 1
    
    if trials > 20:
        print(f'the run is taking too long, I give up.')
        break

print(f'got result from compute job: {result}')
