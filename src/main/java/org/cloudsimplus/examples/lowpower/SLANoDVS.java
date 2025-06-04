package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.Cloudlet.Status;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.power.models.PowerModelHostSimple;
import org.cloudsimplus.provisioners.ResourceProvisionerSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.slametrics.SlaContract;
import org.cloudsimplus.utilizationmodels.UtilizationModel;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The method proposed in this paper.
 */
public final class SLANoDVS {
    private final List<Datacenter> datacenterList;
    private final List<Vm> vmList = new ArrayList<>(LowPower.VMS);
    private CloudSimPlus simulation;

    private final SlaContract contract;
    private final List<Host> allHostList;
    private final List<Cloudlet> cloudletList;
    private final List<Cloudlet> failedTasks = new ArrayList<>();

    public static void main(String[] args) {
        new SLANoDVS();
    }

    private SLANoDVS() {
        System.out.println("Starting " + getClass().getSimpleName());
        simulation = new CloudSimPlus();

        this.contract = SlaContract.getInstance(LowPower.CUSTOMER_SLA_CONTRACT);
        this.cloudletList = new ArrayList<>(LowPower.CLOUDLETS);
        this.allHostList = new ArrayList<>(LowPower.HOSTS);
        this.datacenterList = new ArrayList<>(LowPower.DATACENTERS);

        for (int i = 0; i < LowPower.DATACENTERS; i++)
            this.datacenterList.add(createDatacenter());

        final var broker = new TaskScheduler(simulation);

        createAndSubmitVms(broker);
        createAndSubmitCloudlets(broker);

        simulation.addOnClockTickListener(this::simulationTick);
        simulation.start();

        // Fail the tasks after the simulation is done
        for (Cloudlet c : failedTasks)
            c.setStatus(Status.FAILED);
        new CloudletsTableBuilder(cloudletList).build();

        LowPower.printHostsCpuUtilizationAndPowerConsumption(allHostList);
        System.out.println(getClass().getSimpleName() + " finished!");
    }

    /**
     * Creates the tasks to be sent to system
     */
    private void createAndSubmitCloudlets(final DatacenterBroker broker) {
        double currentArrivalTime = 0;
        final UtilizationModel um = new UtilizationModelDynamic(UtilizationModel.Unit.ABSOLUTE, 50);
        for (int i = 1; i <= LowPower.CLOUDLETS; i++) {
            UtilizationModelDynamic cpuUtilizationModel = new UtilizationModelDynamic(
                    LowPower.CLOUDLET_CPU_USAGE_PERCENT);

            final int deadline = 5 + LowPower.rng.nextInt(5);
            final Cloudlet c = new DeadlineTask(
                    i, LowPower.CLOUDLET_LENGHT, 1, deadline)
                    .setFileSize(LowPower.CLOUDLET_FILESIZE)
                    .setOutputSize(LowPower.CLOUDLET_OUTPUTSIZE)
                    .setUtilizationModelCpu(cpuUtilizationModel)
                    .setUtilizationModelRam(um)
                    .setUtilizationModelBw(um);
            c.setSubmissionDelay(currentArrivalTime);
            c.addOnFinishListener(this::taskFinishedCallback);
            // Random arrival time
            currentArrivalTime += (double) LowPower.rng.nextInt(5) / 10;
            cloudletList.add(c);
        }

        broker.submitCloudletList(cloudletList);
    }

    /**
     * Creates the virtual machines to run on each host
     */
    private void createAndSubmitVms(final DatacenterBroker broker) {
        for (int i = 0; i < LowPower.VMS; i++) {
            final Vm vm = new VmSimple(vmList.size(), LowPower.VM_MIPS[LowPower.rng.nextInt(LowPower.VM_MIPS.length)],
                    LowPower.VM_PES_NUM)
                    .setRam(LowPower.VM_RAM).setBw(LowPower.VM_BW).setSize(LowPower.VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vm.enableUtilizationStats();
            vmList.add(vm);
        }
        broker.submitVmList(vmList);
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
            final var host = new ScoredPM(LowPower.HOST_RAM, LowPower.HOST_BW, LowPower.HOST_STORAGE, peList, LowPower.MTTF);
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
    private void taskFinishedCallback(CloudletVmEventInfo task) {
        final ScoredPM physicalMachine = (ScoredPM) task.getVm().getHost();

        if (LowPower.FAILURE_RNG.eventsHappened()) {
            System.out.println("FAILED task " + task.getCloudlet().getId());
            failedTasks.add(task.getCloudlet());
            // We don't reschedule the task in round robin

            physicalMachine.taskDone(false);
        } else {
            physicalMachine.taskDone(true);
        }
    }

    /**
     * This is called on every tick of the simulation
     * We will update the priority of each task in some ticks.
     */
    private void simulationTick(EventInfo info) {
        if (((long) info.getTime()) % LowPower.T_p == 0) {
            for (Cloudlet c : cloudletList) {
                if (c.getStatus() != Status.SUCCESS) {
                    ((DeadlineTask) c).refreshPriority(info.getTime());
                }
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
    private static final class TaskScheduler extends DatacenterBrokerSimple {
        public TaskScheduler(final CloudSimPlus simulation) {
            super(simulation, "TaskScheduler");
        }

        @Override
        protected Vm defaultVmMapper(Cloudlet task) {
            // If this task is assigned to a VM
            if (task.isBoundToVm()) {
                return task.getVm();
            }

            // Get list of all VMs and calculate the score of them
            if (getVmExecList().isEmpty())
                return Vm.NULL;

            // Get the VM which is running on a host that has the best score
            return getVmExecList().stream().max(Comparator.comparing(c -> ((ScoredPM) (((Vm) c).getHost())).getScore()))
                    .get();
        }
    }

    /**
     * A task which has a deadline and its priority is tied to the deadline
     */
    private static final class DeadlineTask extends CloudletSimple {
        private final double deadline;

        public DeadlineTask(final long id, final long length, final long pesNumber, final double deadline) {
            super(id, length, pesNumber);
            this.deadline = deadline;
        }

        /**
         * Refereshes the priority based on the remaning time to do this task
         * 
         * @param currentTime The current time of the simulation
         */
        public void refreshPriority(final double currentTime) {
            final double remainingTime = deadline - currentTime;
            if (remainingTime < LowPower.T_p)
                setPriority(3);
            else if (remainingTime < 2 * LowPower.T_p)
                setPriority(2);
            else
                setPriority(1);
        }
    }
}