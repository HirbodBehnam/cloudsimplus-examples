package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.Cloudlet.Status;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.examples.lowpower.LowPower.CloudletDedline;
import org.cloudsimplus.examples.lowpower.LowPower.RoundRobinDatacenterAllocator;
import org.cloudsimplus.examples.lowpower.LowPower.VmWithTaskCounter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.power.PowerMeasurement;
import org.cloudsimplus.power.models.PowerModelHostAbstract;
import org.cloudsimplus.provisioners.ResourceProvisionerSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.vms.Vm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

/**
 * The method proposed in this paper.
 */
public final class SLADVSAlgorithm {
    private final List<Datacenter> datacenterList;
    private final List<Vm> vmList;
    private CloudSimPlus simulation;

    private final List<Host> allHostList;
    private final List<LowPower.CloudletDedline> cloudletList;
    private final TaskScheduler broker;

    private final boolean dvsEnabled;
    private final double lastDeadline;
    private final HashSet<Long> processedTicks;

    SLADVSAlgorithm(boolean dvsEnabled) {
        simulation = new CloudSimPlus();

        this.cloudletList = new ArrayList<>(LowPower.CLOUDLETS);
        this.allHostList = new ArrayList<>(LowPower.HOSTS);
        this.datacenterList = new ArrayList<>(LowPower.DATACENTERS);
        this.vmList = new ArrayList<>(LowPower.VMS);

        this.dvsEnabled = dvsEnabled;

        for (int i = 0; i < LowPower.DATACENTERS; i++)
            this.datacenterList.add(createDatacenter());

        broker = new TaskScheduler(simulation);

        LowPower.createAndSubmitVms(broker, vmList, false);
        LowPower.createCloudlets(cloudletList, this::taskFinishedCallback);

        // We must at least submit one cloudlet apparently
        broker.submitCloudlet(cloudletList.get(0));

        lastDeadline = cloudletList.stream().mapToDouble(CloudletDedline::getDeadline).max().orElseThrow();
        simulation.terminateAt(lastDeadline);
        processedTicks = new HashSet<>((int) lastDeadline);

        simulation.addOnClockTickListener(this::simulationTick);
        simulation.start();

        LowPower.printTaskInformation(cloudletList);

        LowPower.printHostsCpuUtilizationAndPowerConsumption(allHostList);
        System.out.println(getClass().getSimpleName() + " finished!");
    }

    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(LowPower.HOSTS);
        for (int i = 0; i < LowPower.HOSTS; i++) {
            // Create the cpu cores
            final var peList = new ArrayList<Pe>(LowPower.HOST_NUMBER_OF_PES);
            for (int j = 0; j < LowPower.HOST_NUMBER_OF_PES; j++) {
                peList.add(new PeSimple(LowPower.HOST_MIPS_BY_PE));
            }
            // Create the physical machine
            final var host = new ScoredPM(LowPower.HOST_RAM, LowPower.HOST_BW, LowPower.HOST_STORAGE, peList,
                    LowPower.MTTF);
            host.setPowerModel(new DVFSPowerModel(1000, 100));
            host.setRamProvisioner(new ResourceProvisionerSimple());
            host.setBwProvisioner(new ResourceProvisionerSimple());
            host.setVmScheduler(new VmSchedulerTimeShared());
            host.enableUtilizationStats();
            // Add it to list of machines
            hostList.add(host);
            allHostList.add(host);
        }

        return new DatacenterSimple(simulation, hostList);
    }

    /**
     * When each tasks finishes, we might fail it based on a random number
     */
    private void taskFinishedCallback(CloudletVmEventInfo cloudletInfo) {
        final LowPower.CloudletDedline task = (LowPower.CloudletDedline) cloudletInfo.getCloudlet();
        final LowPower.VmWithTaskCounter virtualMachine = (LowPower.VmWithTaskCounter) cloudletInfo.getVm();
        final ScoredPM physicalMachine = (ScoredPM) virtualMachine.getHost();

        System.out.println("Task " + task.getId() + " done");
        virtualMachine.finishTask(task.getId());

        if (LowPower.FAILURE_RNG.eventsHappened()) {
            System.out.println("FAILED task " + task.getId());
            physicalMachine.taskDone(false);
            task.failed();

            // Reschedule the task
            broker.submitCloudlet(task);
        } else {
            physicalMachine.taskDone(true);

            // Remove tasks that failed once
            task.succeed();
        }
    }

    /**
     * This is called on every tick of the simulation
     * We will update the priority of each task in some ticks.
     * It will also submit the tasks which arrive.
     */
    private void simulationTick(EventInfo event) {
        System.out.println("TICK " + event.getTime());

        LowPower.logStatistics((int) event.getTime(), cloudletList, allHostList);

        if ((long) event.getTime() % LowPower.T_p == 0 && !processedTicks.contains((long) event.getTime())) {
            // Referesh task priority
            for (Cloudlet c : cloudletList) {
                if (c.getStatus() != Status.SUCCESS) {
                    ((LowPower.CloudletDedline) c).refreshPriority(event.getTime());
                }
            }

            // DVFS VMs
            if (dvsEnabled) {
                List<Vm> dvsVMs = vmList.stream()
                        .sorted(Comparator.comparing(c -> ((ScoredPM) c.getHost()).getScore(),
                                Comparator.reverseOrder()))
                        .toList()
                        .subList(0, vmList.size() * 7 / 10);
                for (Vm vm : dvsVMs) {
                    // See the maxinum amount we can DVFS
                    double hostMIPS = vm.getMips();
                    double dvfsFactor = 0;
                    for (Cloudlet c : ((VmWithTaskCounter) vm).getAllocatedTasks()) {
                        double timeRunning = c.getFinishedLengthSoFar() / hostMIPS;
                        double totalTime = c.getLength() / hostMIPS;
                        double timeLeft = totalTime - timeRunning;
                        if (timeLeft < 0) // Just a failsafe
                            throw new RuntimeException("negative timeLeft in DVFS");
                        double timeToDeadline = ((LowPower.CloudletDedline) c).getDeadline() - event.getTime();
                        if (timeToDeadline <= 0) { // Deadline missed :(
                            dvfsFactor = 1;
                            break;
                        }
                        double newDvfsFactor = timeLeft / timeToDeadline;
                        dvfsFactor = Math.min(1, Math.max(dvfsFactor, newDvfsFactor));
                    }
                    dvfsFactor = Math.max(LowPower.MIN_DVS_RATIO, Math.min(dvfsFactor * 1.1, 1));
                    // Enable DVFS
                    for (Pe pe : vm.getHost().getPeList()) {
                        long newAllocatedResources = (long) (Math.ceil(pe.getCapacity() * dvfsFactor));
                        pe.setAllocatedResource(newAllocatedResources);
                    }
                    ((DVFSPowerModel) vm.getHost().getPowerModel()).addDvfsTick(dvfsFactor, event.getTime());
                }
            }

            // Do not process this tick again
            processedTicks.add((long) event.getTime());
        }

        for (Cloudlet task : cloudletList) {
            if (event.getTime() >= ((LowPower.CloudletDedline) task).getArrivalTime()
                    && task.getStatus() == Status.INSTANTIATED) {
                System.out.println("Submitting task " + task.getId() + " with status " + task.getStatus().toString());
                this.broker.submitCloudlet(task);
            }
        }
    }

    /**
     * The scheduler proposed in this paper
     */
    private static final class TaskScheduler extends RoundRobinDatacenterAllocator {
        public TaskScheduler(final CloudSimPlus simulation) {
            super(simulation);
        }

        @Override
        protected Vm defaultVmMapper(Cloudlet task) {
            System.out.println("Mapping task " + task.getId());
            // If this task is assigned to a VM, just return that
            if (task.isBoundToVm()) {
                System.out.println("TASK ALREADY MAPPED");
                return task.getVm();
            }
            if (getVmExecList().isEmpty())
                return Vm.NULL;

            final LowPower.CloudletDedline castedTask = (LowPower.CloudletDedline) task;

            // If this task has failed a lot of times, schedule it in other datacenters
            if (castedTask.getFailedCount() < 2) {
                // Sort all virtual machines by their score
                List<Vm> sortedVms = getVmExecList().stream()
                        .filter(vm -> vm.getHost().getDatacenter().getId() == castedTask.getClosestDatacenter())
                        .sorted(Comparator.comparing(c -> ((ScoredPM) c.getHost()).getScore(),
                                Comparator.reverseOrder()))
                        .toList();
                if (sortedVms.size() == 0)
                    throw new RuntimeException(
                            "Invalid datacenter ID? Need " + castedTask.getClosestDatacenter());

                // Schedule in the datacenter
                Vm selectedVm = selectVmFromList(sortedVms, task);
                if (selectedVm != null) {
                    System.out.println("Mapping task " + task.getId() + " to VM " + selectedVm.getId());
                    return selectedVm;
                }
                System.out.println("Cannot map task " + task.getId() + " inside datacenter");
            }

            // External scheduling
            List<Vm> sortedVms = getVmExecList().stream()
                    .filter(vm -> vm.getHost().getDatacenter().getId() != castedTask.getClosestDatacenter())
                    .sorted(Comparator.comparing(c -> ((ScoredPM) c.getHost()).getScore(), Comparator.reverseOrder()))
                    .toList();
            Vm selectedVm = selectVmFromList(sortedVms, task);
            if (selectedVm != null) {
                System.out.println("Mapping task " + task.getId() + " to VM " + selectedVm.getId());
                return selectedVm;
            }
            System.out.println("Cannot map task " + task.getId());

            return Vm.NULL;
        }

        private static Vm selectVmFromList(List<Vm> sortedVms, Cloudlet task) {
            // Based on the priority, find a suitable VM
            int searchIndex;
            switch (task.getPriority()) {
                case 1: // Low
                    searchIndex = sortedVms.size() * 7 / 10;
                    break;
                case 2:
                    searchIndex = sortedVms.size() * 4 / 10;
                    break;
                default:
                    searchIndex = 0;
                    break;
            }
            for (; searchIndex < sortedVms.size(); searchIndex++) {
                final LowPower.VmWithTaskCounter vm = (LowPower.VmWithTaskCounter) sortedVms.get(searchIndex);
                if (vm.canHandleTask()) {
                    vm.addTask(task);
                    return vm;
                }
            }

            return null;
        }
    }

    /**
     * A physical machine that holds a score. The score is calculated based on the
     * number of tasks done successfully and failed tasks.
     */
    private static final class ScoredPM extends HostSimple {
        private int successfulTasks = 0, failedTasks = 0;
        private final double F_p;

        public ScoredPM(final long ram, final long bw, final long storage, final List<Pe> peList, final double mttf) {
            super(ram, bw, storage, peList);
            this.F_p = 1 - Math.exp(-8760 / mttf);
        }

        public double getScore() {
            return getMips() * getPesNumber() * successfulTasks / (successfulTasks + failedTasks) / (1 - F_p);
        }

        /**
         * When a task is finished this method shall be called
         * 
         * @param successful True if task was sucessful otherwise false
         */
        public void taskDone(boolean successful) {
            if (successful)
                successfulTasks++;
            else
                failedTasks++;
        }
    }

    /**
     * PowerModelHostSimple does not account voltage scaling thus we do it here
     */
    private final class DVFSPowerModel extends PowerModelHostAbstract {
        private static final class DVFSTuple {
            /**
             * Fraction is the fraction of DVFS which is applied (from 0 to 1)
             * and time is the clock tick which that is applied.
             */
            public final double fraction, time;

            public DVFSTuple(double fraction, double time) {
                this.time = time;
                this.fraction = fraction;
            }
        }

        private final double maxPower;
        private final double staticPower;

        /**
         * In each tick that we change the dvfs parameters, we should all one entry to
         * this list
         */
        private final ArrayList<DVFSTuple> dvfsFractions = new ArrayList<>();

        public DVFSPowerModel(final double maxPower, final double staticPower) {
            super();
            if (maxPower < staticPower) {
                throw new IllegalArgumentException("maxPower has to be higher than staticPower");
            }

            this.maxPower = validatePower(maxPower, "maxPower");
            this.staticPower = validatePower(staticPower, "staticPower");
            // We start the simulation with full speed
            dvfsFractions.add(new DVFSTuple(1, 0));
        }

        /**
         * Each time we set DVFS to this host, this method shall be called
         * 
         * @param fraction Fraction of the DVFS factor
         * @param time     The time in simulation tick that we have applied this dvfs
         */
        public void addDvfsTick(double fraction, double time) {
            if (fraction > 1)
                throw new IllegalArgumentException("fraction must be smaller or equal to one");
            dvfsFractions.add(new DVFSTuple(fraction, time));
        }

        @Override
        public PowerMeasurement getPowerMeasurement() {
            final var host = getHost();
            if (!host.isActive()) {
                return new PowerMeasurement();
            }

            final double usageFraction = host.getCpuMipsUtilization() / host.getTotalMipsCapacity();
            return new PowerMeasurement(staticPower, dynamicPower(usageFraction));
        }

        @Override
        public double getPowerInternal(final double utilizationFraction) {
            return staticPower + dynamicPower(utilizationFraction);
        }

        /**
         * Computes the dynamic power consumed according to the CPU utilization
         * percentage.
         * 
         * @param utilizationFraction the utilization percentage (between [0 and 1]) of
         *                            the host.
         * @return the dynamic power supply in Watts (W)
         */
        private double dynamicPower(final double utilizationFraction) {
            if (lastDeadline < dvfsFractions.get(dvfsFractions.size() - 1).time)
                throw new RuntimeException("total runtime is less than last dvfs time: " + lastDeadline + " and "
                        + dvfsFractions.get(dvfsFractions.size() - 1).time);
            double dvfsFraction = 0;
            // Do weighted average
            for (int i = 1; i < dvfsFractions.size(); i++)
                dvfsFraction += dvfsFractions.get(i - 1).fraction
                        * (dvfsFractions.get(i).time - dvfsFractions.get(i - 1).time);
            dvfsFraction += dvfsFractions.get(dvfsFractions.size() - 1).fraction
                    * (lastDeadline - dvfsFractions.get(dvfsFractions.size() - 1).time);
            dvfsFraction /= lastDeadline;
            if (dvfsFraction > 1)
                throw new RuntimeException("more than 1 dvfs factor of " + dvfsFraction);
            // throw new RuntimeException("factor: " + dvfsFraction);
            return (maxPower - staticPower) * utilizationFraction * Math.pow(dvfsFraction, 3);
        }
    }
}