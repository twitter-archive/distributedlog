Storage
=======

This describes some implementation details of storage layer.

Ensemble Placement Policy
-------------------------

`EnsemblePlacementPolicy` encapsulates the algorithm that bookkeeper client uses to select a number of bookies from the
cluster as an ensemble for storing data. The algorithm is typically based on the data input as well as the network
topology properties.

By default, BookKeeper offers a `RackawareEnsemblePlacementPolicy` for placing the data across racks within a
datacenter, and a `RegionAwareEnsemblePlacementPolicy` for placing the data across multiple datacenters.

How does EnsemblePlacementPolicy work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The interface of `EnsemblePlacementPolicy` is described as below.

::

    public interface EnsemblePlacementPolicy {

        /**
         * Initialize the policy.
         *
         * @param conf client configuration
         * @param optionalDnsResolver dns resolver
         * @param hashedWheelTimer timer
         * @param featureProvider feature provider
         * @param statsLogger stats logger
         * @param alertStatsLogger stats logger for alerts
         */
        public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                                  Optional<DNSToSwitchMapping> optionalDnsResolver,
                                                  HashedWheelTimer hashedWheelTimer,
                                                  FeatureProvider featureProvider,
                                                  StatsLogger statsLogger,
                                                  AlertStatsLogger alertStatsLogger);

        /**
         * Uninitialize the policy
         */
        public void uninitalize();

        /**
         * A consistent view of the cluster (what bookies are available as writable, what bookies are available as
         * readonly) is updated when any changes happen in the cluster.
         *
         * @param writableBookies
         *          All the bookies in the cluster available for write/read.
         * @param readOnlyBookies
         *          All the bookies in the cluster available for readonly.
         * @return the dead bookies during this cluster change.
         */
        public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
                                                         Set<BookieSocketAddress> readOnlyBookies);

        /**
         * Choose <i>numBookies</i> bookies for ensemble. If the count is more than the number of available
         * nodes, {@link BKNotEnoughBookiesException} is thrown.
         *
         * @param ensembleSize
         *          Ensemble Size
         * @param writeQuorumSize
         *          Write Quorum Size
         * @param excludeBookies
         *          Bookies that should not be considered as targets.
         * @return list of bookies chosen as targets.
         * @throws BKNotEnoughBookiesException if not enough bookies available.
         */
        public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                                          Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;

        /**
         * Choose a new bookie to replace <i>bookieToReplace</i>. If no bookie available in the cluster,
         * {@link BKNotEnoughBookiesException} is thrown.
         *
         * @param bookieToReplace
         *          bookie to replace
         * @param excludeBookies
         *          bookies that should not be considered as candidate.
         * @return the bookie chosen as target.
         * @throws BKNotEnoughBookiesException
         */
        public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                                 Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace,
                                                 Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;

        /**
         * Reorder the read sequence of a given write quorum <i>writeSet</i>.
         *
         * @param ensemble
         *          Ensemble to read entries.
         * @param writeSet
         *          Write quorum to read entries.
         * @param bookieFailureHistory
         *          Observed failures on the bookies
         * @return read sequence of bookies
         */
        public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble,
                                                 List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory);


        /**
         * Reorder the read last add confirmed sequence of a given write quorum <i>writeSet</i>.
         *
         * @param ensemble
         *          Ensemble to read entries.
         * @param writeSet
         *          Write quorum to read entries.
         * @param bookieFailureHistory
         *          Observed failures on the bookies
         * @return read sequence of bookies
         */
        public List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble,
                                                List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory);
    }

The methods in this interface covers three parts - 1) initialization and uninitialization; 2) how to choose bookies to
place data; and 3) how to choose bookies to do speculative reads.

Initialization and uninitialization
___________________________________

The ensemble placement policy is constructed by jvm reflection during constructing bookkeeper client. After the
`EnsemblePlacementPolicy` is constructed, bookkeeper client will call `#initialize` to initialize the placement policy.

The `#initialize` method takes a few resources from bookkeeper for instantiating itself. These resources include:

1. `ClientConfiguration` : The client configuration that used for constructing the bookkeeper client. The implementation of the placement policy could obtain its settings from this configuration.
2. `DNSToSwitchMapping`: The DNS resolver for the ensemble policy to build the network topology of the bookies cluster. It is optional.
3. `HashedWheelTimer`: A hashed wheel timer that could be used for timing related work. For example, a stabilize network topology could use it to delay network topology changes to reduce impacts of flapping bookie registrations due to zk session expires.
4. `FeatureProvider`: A feature provider that the policy could use for enabling or disabling its offered features. For example, a region-aware placement policy could offer features to disable placing data to a specific region at runtime.
5. `StatsLogger`: A stats logger for exposing stats.
6. `AlertStatsLogger`: An alert stats logger for exposing critical stats that needs to be alerted.

The ensemble placement policy is a single instance per bookkeeper client. The instance will be `#uninitialize` when
closing the bookkeeper client. The implementation of a placement policy should be responsible for releasing all the
resources that allocated during `#initialize`.





