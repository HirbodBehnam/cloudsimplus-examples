package org.cloudsimplus.examples.lowpower;

import java.util.List;
import java.util.Random;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.distributions.PoissonDistr;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.utilizationmodels.UtilizationModel;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.HostResourceStats;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

public final class LowPower {
    public static final int HOST_MIPS_BY_PE = 3000;
    public static final int HOST_NUMBER_OF_PES = 1;
    public static final long HOST_RAM = 8 * 1024; // host memory (MB)
    public static final long HOST_STORAGE = 1024 * 1024; // host storage
    public static final long HOST_BW = 100000000L;

    public static final int[] VM_MIPS = { 1000, 2000, 3000 };
    public static final long VM_SIZE = 1000; // image size (MB)
    public static final long VM_RAM = HOST_RAM; // vm memory (MB)
    public static final long VM_BW = 100000;
    public static final int VM_PES_NUM = HOST_NUMBER_OF_PES; // number of cpus

    public static final long CLOUDLET_LENGHT = 1500;
    public static final long CLOUDLET_FILESIZE = 300 * 1024;
    public static final long CLOUDLET_OUTPUTSIZE = 300 * 1024;
    public static final double CLOUDLET_CPU_USAGE_PERCENT = 0.75;

    public static final int DATACENTERS = 3;
    public static final int HOSTS = 100;
    /**
     * In this simulation, each VM will be mapped to one host and multiple tasks
     * will be assigned to each VM.
     */
    public static final int VMS = HOSTS * DATACENTERS;
    public static final int CLOUDLETS = 5 * VMS;
    public static final int SCHEDULE_TIME_TO_PROCESS_DATACENTER_EVENTS = 5;
    /**
     * Equation 15 looks wrong to me so I changed it. At the very first, the workload
     * of the datacenter is zero. Thus equation 15 will always return 0. On the otherhand,
     * the text above it suggests that the second Wl_mean is another variable. Thus, I
     * will simply replace Wl_mean / alpha with this variable
     */
    public static final double ALPHA_WORKLOAD = 0.5;

    public static final Random rng = new Random();

    public static final double MTTF = 8640;
    public static final PoissonDistr FAILURE_RNG = new PoissonDistr(1 / MTTF);
    public static final double SIMULATION_INTERVAL = 1;

    /**
     * The cycle which we renew the priority of each task
     */
    public static final long T_p = 100;

    /**
     * For a set of hosts, writes their CPU utlization
     * 
     * @param hostList The list to print their CPU utilization
     */
    public static void printHostsCpuUtilizationAndPowerConsumption(final List<Host> hostList) {
        System.out.println("Host Power Consumptions:");
        for (final Host host : hostList) {
            final HostResourceStats cpuStats = host.getCpuUtilizationStats();

            // The total Host's CPU utilization for the time specified by the map key
            final double utilizationPercentMean = cpuStats.getMean();
            final double watts = host.getPowerModel().getPower(utilizationPercentMean);
            System.out.printf(
                    "Host %2d in datacenter %d CPU Usage mean: %6.1f%% | Power Consumption mean: %8.0f W%n",
                    host.getId(), host.getDatacenter().getId(), utilizationPercentMean * 100, watts);
        }
        System.out.println();
    }

    /**
     * Creates the virtual machines to run on each host
     */
    static void createAndSubmitVms(DatacenterBroker broker, List<Vm> vmList) {
        for (int i = 0; i < LowPower.VMS; i++) {
            final int maximumTasks = rng.nextInt(2, 4);
            final Vm vm = new VmWithTaskCounter(vmList.size(),
                    LowPower.VM_MIPS[LowPower.rng.nextInt(LowPower.VM_MIPS.length)],
                    LowPower.VM_PES_NUM, maximumTasks)
                    .setRam(LowPower.VM_RAM).setBw(LowPower.VM_BW).setSize(LowPower.VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vm.enableUtilizationStats();
            vmList.add(vm);
        }
        broker.submitVmList(vmList);
    }

    /**
     * Creates the tasks to be sent to system
     */
    static void createCloudlets(List<Cloudlet> cloudletList, EventListener<CloudletVmEventInfo> onFinishListener) {
        long currentArrivalTime = 0;
        final UtilizationModel um = new UtilizationModelDynamic(UtilizationModel.Unit.ABSOLUTE, 50);
        for (int i = 1; i <= CLOUDLETS; i++) {
            UtilizationModelDynamic cpuUtilizationModel = new UtilizationModelDynamic(
                    CLOUDLET_CPU_USAGE_PERCENT);

            final long deadline = 25 + rng.nextInt(25) + currentArrivalTime;
            final int closestDatacenter = rng.nextInt(DATACENTERS) + 1;
            final Cloudlet c = new CloudletDedline(
                    i, CLOUDLET_LENGHT, 1, deadline, currentArrivalTime, closestDatacenter)
                    .setFileSize(CLOUDLET_FILESIZE)
                    .setOutputSize(CLOUDLET_OUTPUTSIZE)
                    .setUtilizationModelCpu(cpuUtilizationModel)
                    .setUtilizationModelRam(um)
                    .setUtilizationModelBw(um);
            c.addOnFinishListener(onFinishListener);
            // Random arrival time
            currentArrivalTime += rng.nextInt(5);
            cloudletList.add(c);
        }
    }

    /**
     * Based on equation 14, we need to keep track of maximum number of tasks that
     * each VM can
     * handle.
     */
    static final class VmWithTaskCounter extends VmSimple {
        private final int maxExecutingTasks;
        private int currentExecutingTasks = 0;

        public VmWithTaskCounter(long id, long mipsCapacity, long pesNumber, int maxExecutingTasks) {
            super(id, mipsCapacity, pesNumber);
            this.maxExecutingTasks = maxExecutingTasks;
        }

        /**
         * This function will execute formula 13 of the paper to check if
         * this VM can handle a task or not
         * @return True if it can handle it, otherwise false
         */
        public boolean canHandleTask() {
            // If we are full on number of tasks, just return false
            if (currentExecutingTasks >= maxExecutingTasks)
                return false;
            // Otherwise, check datacenter workload
            final double hostWorkload = getHost().getCpuPercentUtilization();
            final double datacenterWorkload = getHost().getDatacenter().getHostList()
                .stream()
                .mapToDouble(pm -> pm.getCpuPercentUtilization())
                .average()
                .orElse(Double.NaN);
            return hostWorkload <= datacenterWorkload + ALPHA_WORKLOAD;
        }

        public void addTask() {
            currentExecutingTasks++;
        }

        public void finishTask() {
            currentExecutingTasks--;
        }
    }

    /**
     * A task which has a deadline and its priority is tied to the deadline
     */
    static final class CloudletDedline extends CloudletSimple {
        private final long arrivalTime, deadline;
        private final int closestDatacenter;
        private int failedCount = 0;

        public CloudletDedline(final long id, final long length, final long pesNumber, final long deadline,
                final long arrivalTime, final int closestDatacenter) {
            super(id, length, pesNumber);
            this.deadline = deadline;
            this.closestDatacenter = closestDatacenter;
            this.arrivalTime = arrivalTime;
            refreshPriority(0);
        }

        public int getClosestDatacenter() {
            return closestDatacenter;
        }

        public long getArrivalTime() {
            return arrivalTime;
        }

        /**
         * Refereshes the priority based on the remaning time to do this task
         * 
         * @param currentTime The current time of the simulation
         */
        public void refreshPriority(final long currentTime) {
            final long remainingTime = deadline - currentTime;
            if (remainingTime < LowPower.T_p)
                setPriority(3); // High
            else if (remainingTime < 2 * LowPower.T_p)
                setPriority(2); // Medium
            else
                setPriority(1); // Low
        }

        /**
         * Indicates that this task has failed
         */
        public void failedTask() {
            failedCount++;
        }

        /**
         * @return How many times this task has failed
         */
        public int getFailedCount() {
            return failedCount;
        }
    }

    /**
     * Each VM must be allocated to one host in each datacenter. In this class,
     * defaultDatacenterMapper
     * is overridden in order to control that each VM will be send to each
     * datacenter in a round robin
     * fasion. This causes each VM to be allocated to one and only host.
     */
    static class RoundRobinDatacenterAllocator extends DatacenterBrokerSimple {
        private int nextDatacenterIndex = 0;

        public RoundRobinDatacenterAllocator(final CloudSimPlus simulation) {
            super(simulation, "RoundRobinDatacenterAllocator");
        }

        @Override
        protected Datacenter defaultDatacenterMapper(final Datacenter lastDatacenter, final Vm vm) {
            final List<Datacenter> datacenters = getDatacenterList();
            final Datacenter result = datacenters.get(nextDatacenterIndex);
            nextDatacenterIndex = (nextDatacenterIndex + 1) % datacenters.size();
            System.out.println("Mapping VM " + vm.getId() + " to datacenter " + result.getId());
            return result;
        }
    }
}
