package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.Cloudlet.Status;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.examples.lowpower.LowPower.RoundRobinDatacenterAllocator;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.power.models.PowerModelHostSimple;
import org.cloudsimplus.provisioners.ResourceProvisionerSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.vms.Vm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The method proposed in this paper.
 */
public final class SLANoDVS {
    private final List<Datacenter> datacenterList;
    private final List<Vm> vmList;
    private CloudSimPlus simulation;
    private long currentTime = 0;

    private final List<Host> allHostList;
    private final List<Cloudlet> cloudletList;
    private final List<Cloudlet> failedTasks = new ArrayList<>();
    private final TaskScheduler broker;

    public static void main(String[] args) {
        new SLANoDVS();
    }

    private SLANoDVS() {
        System.out.println("Starting " + getClass().getSimpleName());
        simulation = new CloudSimPlus();

        this.cloudletList = new ArrayList<>(LowPower.CLOUDLETS);
        this.allHostList = new ArrayList<>(LowPower.HOSTS);
        this.datacenterList = new ArrayList<>(LowPower.DATACENTERS);
        this.vmList = new ArrayList<>(LowPower.VMS);

        for (int i = 0; i < LowPower.DATACENTERS; i++)
            this.datacenterList.add(createDatacenter());

        broker = new TaskScheduler(simulation);

        LowPower.createAndSubmitVms(broker, vmList);
        LowPower.createCloudlets(cloudletList, this::taskFinishedCallback);
        // We must at least submit one cloudlet apparently
        broker.submitCloudlet(cloudletList.get(0));

        simulation.startSync();
        while (simulation.isRunning()) {
            simulationTick(currentTime);
            simulation.runFor(LowPower.SIMULATION_INTERVAL);
            currentTime++;
        }

        new CloudletsTableBuilder(cloudletList).build();

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
            host.setPowerModel(new PowerModelHostSimple(1000, 700));
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
        virtualMachine.finishTask();

        if (LowPower.FAILURE_RNG.eventsHappened()) {
            System.out.println("FAILED task " + task.getId());
            failedTasks.add(task);
            physicalMachine.taskDone(false);
            task.failedTask();

            // Reschedule the task
            broker.submitCloudlet(task);
        } else {
            physicalMachine.taskDone(true);

            // Remove tasks that failed once
            failedTasks.removeIf(t -> task.getId() == t.getId());
        }
    }

    /**
     * This is called on every tick of the simulation
     * We will update the priority of each task in some ticks.
     * It will also submit the tasks which arrive.
     */
    private void simulationTick(long currentTime) {
        if (currentTime % LowPower.T_p == 0) {
            for (Cloudlet c : cloudletList) {
                if (c.getStatus() != Status.SUCCESS) {
                    ((LowPower.CloudletDedline) c).refreshPriority(currentTime);
                }
            }
        }

        for (Cloudlet task : cloudletList) {
            if (currentTime == ((LowPower.CloudletDedline) task).getArrivalTime()) {
                this.broker.submitCloudlet(task);
            }
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

            // If this task has failed a lot of times, schedule it in other datacenters
            if (((LowPower.CloudletDedline) task).getFailedCount() < 2) {

                // Sort all virtual machines by their score
                List<Vm> sortedVms = getVmExecList().stream()
                        .filter(vm -> vm.getHost().getDatacenter().getId() == ((LowPower.CloudletDedline) task)
                                .getClosestDatacenter())
                        .sorted(Comparator.comparing(c -> ((ScoredPM) (((Vm) c).getHost())).getScore()))
                        .toList();
                if (sortedVms.size() == 0)
                    throw new RuntimeException(
                            "Invalid datacenter ID? Need " + ((LowPower.CloudletDedline) task).getClosestDatacenter());

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
                    .filter(vm -> vm.getHost().getDatacenter().getId() != ((LowPower.CloudletDedline) task)
                            .getClosestDatacenter())
                    .sorted(Comparator.comparing(c -> ((ScoredPM) (((Vm) c).getHost())).getScore()))
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
                    vm.addTask();
                    return vm;
                }
            }

            return null;
        }
    }
}