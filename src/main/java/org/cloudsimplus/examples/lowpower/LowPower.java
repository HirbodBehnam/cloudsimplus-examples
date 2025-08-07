package org.cloudsimplus.examples.lowpower;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.Cloudlet.Status;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.distributions.ExponentialDistr;
import org.cloudsimplus.distributions.PoissonDistr;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
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

    public static final long CLOUDLET_LENGHT = 15000;
    public static final long CLOUDLET_FILESIZE = 300 * 1024;
    public static final long CLOUDLET_OUTPUTSIZE = 300 * 1024;
    public static final double CLOUDLET_CPU_USAGE_PERCENT = 0.75;
    public static final long CLOUDLET_EXTERNAL_MIGRATION_OVERHEAD = 1;

    public static final int DATACENTERS = 3;
    public static final int HOSTS = 100;
    /**
     * In this simulation, each VM will be mapped to one host and multiple tasks
     * will be assigned to each VM.
     */
    public static final int VMS = HOSTS * DATACENTERS;
    public static final int CLOUDLETS = 10 * VMS;
    /**
     * The rate of cloudlet creation. (This many tasks per interval)
     */
    public static final double CLOUDLET_ARRIVAL_INTERVAL = 1;
    public static final double CLOUDLET_DEADLINE_SLACK = 25;
    /**
     * Equation 15 looks wrong to me so I changed it. At the very first, the
     * workload
     * of the datacenter is zero. Thus equation 15 will always return 0. On the
     * otherhand,
     * the text above it suggests that the second Wl_mean is another variable. Thus,
     * I
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
     * We can multiply the frequency of each PE by this value
     */
    public static final double MIN_DVS_RATIO = 0.7;

    public static final double EXTERNAL_DATACENTER_SCHEDULE_PENALTY = 20;

    private static class SimulationStatistics {
        private final long failedTasks;
        private final double powerConsumption;

        public SimulationStatistics(long failedTasks, double powerConsumption) {
            this.failedTasks = failedTasks;
            this.powerConsumption = powerConsumption;
        }
    }

    /**
     * We keep the statics related to the simulation here
     */
    private static final TreeMap<Integer, SimulationStatistics> STATISTICS = new TreeMap<>();

    /**
     * For a set of hosts, writes their CPU utlization
     * 
     * @param hostList The list to print their CPU utilization
     */
    static void reportHostsCpuUtilizationAndPowerConsumption(final List<Host> hostList) {
        try {
            PrintWriter outputFile = new PrintWriter(new FileWriter("reports/host.csv"));
            double totalPower = 0;
            outputFile.println("Host ID,Datacenter ID,CPU Usage Mean,Power Consumption Mean");
            for (final Host host : hostList) {
                final HostResourceStats cpuStats = host.getCpuUtilizationStats();

                // The total Host's CPU utilization for the time specified by the map key
                final double utilizationPercentMean = cpuStats.getMean();
                final double watts = host.getPowerModel().getPower(utilizationPercentMean);
                totalPower += watts;
                outputFile.printf(
                        "%d,%d,%f,%f\n",
                        host.getId(),
                        host.getDatacenter().getId(),
                        utilizationPercentMean * 100,
                        watts);
            }
            System.out.printf("Total power: %f W\n", totalPower);
            outputFile.close();
        } catch (IOException e) {
            System.out.println("Cannot write host report");
            e.printStackTrace();
        }
    }

    /**
     * Report information about each task as a csv file
     * 
     * @param tasks List of tasks
     */
    static void reportTaskInformation(final List<CloudletDedline> tasks) {
        try {
            PrintWriter outputFile = new PrintWriter(new FileWriter("reports/tasks.csv"));
            outputFile.println(
                    "Task ID,VM ID,Host ID,Datacenter ID,Closest Datacenter,Arrival Time,Start Time,Finish Time,Deadline,Failed Count,Failed");
            int failedCount = 0;
            for (CloudletDedline task : tasks) {
                boolean failed = task.isFailed();
                outputFile.printf("%d,%d,%d,%d,%d,%f,%f,%f,%f,%d,%d\n",
                        task.getId(),
                        task.getVm().getId(),
                        task.getVm().getHost().getId(),
                        task.getVm().getHost().getDatacenter().getId(),
                        task.getClosestDatacenter(),
                        task.getArrivalTime(),
                        task.getStartTime(),
                        task.getFinishTime(),
                        task.getDeadline(),
                        task.getFailedCount(),
                        failed ? 1 : 0);
                if (failed)
                    failedCount++;
            }
            System.out.println("Failure rate: " + ((double) failedCount) / tasks.size() + " %");
            outputFile.close();
        } catch (IOException e) {
            System.out.println("Cannot write task report");
            e.printStackTrace();
        }
    }

    /**
     * Reports data saved in STATISTICS to a file
     */
    static void reportTimedMetrics() {
        try {
            PrintWriter outputFile = new PrintWriter(new FileWriter("reports/timed-stats.csv"));
            outputFile.println("Tick,Power,Failed");
            for (final var entry : STATISTICS.entrySet()) {
                outputFile.printf("%d,%f,%d\n",
                        entry.getKey(), entry.getValue().powerConsumption, entry.getValue().failedTasks);
            }
            outputFile.close();
        } catch (IOException e) {
            System.out.println("Cannot write task report");
            e.printStackTrace();
        }
    }

    /**
     * Creates the virtual machines to run on each host
     */
    static void createAndSubmitVms(DatacenterBroker broker, List<Vm> vmList, boolean queued) {
        for (int i = 0; i < VMS; i++) {
            final int maximumTasks = rng.nextInt(2, 3);
            final Vm vm = new VmWithTaskCounter(vmList.size(),
                    VM_MIPS[rng.nextInt(VM_MIPS.length)],
                    VM_PES_NUM, maximumTasks)
                    .setRam(VM_RAM).setBw(VM_BW).setSize(VM_SIZE)
                    .setCloudletScheduler(
                            queued ? new CloudletSchedulerSpaceShared() : new CloudletSchedulerTimeShared());
            vm.enableUtilizationStats();
            vmList.add(vm);
        }
        broker.submitVmList(vmList);
    }

    /**
     * Creates the tasks to be sent to system
     */
    static void createCloudlets(List<CloudletDedline> cloudletList,
            EventListener<CloudletVmEventInfo> onFinishListener) {
        double currentArrivalTime = 0;
        final ExponentialDistr arrivalTimeIntervals = new ExponentialDistr(CLOUDLET_ARRIVAL_INTERVAL);
        final UtilizationModel um = new UtilizationModelDynamic(UtilizationModel.Unit.ABSOLUTE, 50);

        for (int i = 1; i <= CLOUDLETS; i++) {
            UtilizationModelDynamic cpuUtilizationModel = new UtilizationModelDynamic(
                    CLOUDLET_CPU_USAGE_PERCENT);

            final double deadline = CLOUDLET_DEADLINE_SLACK + rng.nextInt((int) CLOUDLET_DEADLINE_SLACK)
                    + currentArrivalTime;
            final int closestDatacenter = rng.nextInt(DATACENTERS) + 1;
            final CloudletDedline c = (CloudletDedline) new CloudletDedline(
                    i, CLOUDLET_LENGHT, 1, deadline, currentArrivalTime, closestDatacenter)
                    .setFileSize(CLOUDLET_FILESIZE)
                    .setOutputSize(CLOUDLET_OUTPUTSIZE)
                    .setUtilizationModelCpu(cpuUtilizationModel)
                    .setUtilizationModelRam(um)
                    .setUtilizationModelBw(um);
            c.addOnFinishListener(onFinishListener);
            // Random arrival time
            currentArrivalTime += arrivalTimeIntervals.sample();
            cloudletList.add(c);
        }
    }

    /**
     * Logs the statistics of a simulation tick
     */
    static void logStatistics(int currentTime, final List<CloudletDedline> tasks, final List<Host> hosts) {
        // Do not log the first second
        if (currentTime == 0)
            return;
        // Do not re-insert
        if (STATISTICS.containsKey(currentTime))
            return;

        STATISTICS.put(currentTime, new SimulationStatistics(
                tasks.stream().filter(t -> t.getStatus() == Status.SUCCESS && t.isFailed()).count(),
                hosts.stream().mapToDouble(h -> h.getPowerModel().getPower(h.getCpuUtilizationStats().getMean()))
                        .filter(p -> !Double.isNaN(p)).average().orElse(0)));
    }

    /**
     * Based on equation 14, we need to keep track of maximum number of tasks that
     * each VM can handle.
     */
    static final class VmWithTaskCounter extends VmSimple {
        private final int maxExecutingTasks;
        // Map from ID to task
        private final HashMap<Long, Cloudlet> currentExecutingTasks;

        public VmWithTaskCounter(long id, long mipsCapacity, long pesNumber, int maxExecutingTasks) {
            super(id, mipsCapacity, pesNumber);
            this.maxExecutingTasks = maxExecutingTasks;
            this.currentExecutingTasks = new HashMap<>(maxExecutingTasks);
        }

        /**
         * This function will execute formula 13 of the paper to check if
         * this VM can handle a task or not
         * 
         * @return True if it can handle it, otherwise false
         */
        public boolean canHandleTask() {
            // If we are full on number of tasks, just return false
            if (currentExecutingTasks.size() >= maxExecutingTasks)
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

        public void addTask(Cloudlet task) {
            currentExecutingTasks.put(task.getId(), task);
        }

        public void finishTask(long taskID) {
            currentExecutingTasks.remove(taskID);
        }

        public List<Cloudlet> getAllocatedTasks() {
            return new ArrayList<>(currentExecutingTasks.values());
        }
    }

    /**
     * A task which has a deadline and its priority is tied to the deadline
     */
    static final class CloudletDedline extends CloudletSimple {
        private final double arrivalTime, deadline;
        private final int closestDatacenter;
        private int failedCount = 0;
        private boolean failed;

        public CloudletDedline(final long id, final long length, final long pesNumber, final double deadline,
                final double arrivalTime, final int closestDatacenter) {
            super(id, length, pesNumber);
            this.deadline = deadline;
            this.closestDatacenter = closestDatacenter;
            this.arrivalTime = arrivalTime;
            refreshPriority(0);
        }

        public int getClosestDatacenter() {
            return closestDatacenter;
        }

        public double getDeadline() {
            return deadline;
        }

        public double getArrivalTime() {
            return arrivalTime;
        }

        /**
         * Refereshes the priority based on the remaning time to do this task
         * 
         * @param currentTime The current time of the simulation
         */
        public void refreshPriority(final double currentTime) {
            final double remainingTime = deadline - currentTime;
            if (remainingTime < T_p)
                setPriority(3); // High
            else if (remainingTime < 2 * T_p)
                setPriority(2); // Medium
            else
                setPriority(1); // Low
        }

        /**
         * Indicates that this task has failed
         */
        public void failed() {
            failedCount++;
            failed = true;
        }

        /**
         * This method must be called when task is successfully ran
         */
        public void succeed() {
            failed = false;
        }

        /**
         * @return How many times this task has failed
         */
        public int getFailedCount() {
            return failedCount;
        }

        /**
         * This method will check if the task has failed based on its deadline and
         * status.
         * 
         * @return True if the task has failed
         */
        public boolean isFailed() {
            // The task must internally succeed in the cloudsim in order to be processed
            if (getStatus() != Status.SUCCESS)
                return true;
            // Manually failed task
            if (failed)
                return true;
            // Scheduled outside datacenter?
            final double exeternalDatacenterPenalty = getVm().getHost().getDatacenter().getId() != closestDatacenter
                    ? EXTERNAL_DATACENTER_SCHEDULE_PENALTY
                    : 0;
            // Check deadline
            return getFinishTime() + exeternalDatacenterPenalty > deadline;
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
