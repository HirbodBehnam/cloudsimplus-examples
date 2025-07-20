package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.cloudlets.Cloudlet.Status;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.power.models.PowerModelHostSimple;
import org.cloudsimplus.provisioners.ResourceProvisionerSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.utilizationmodels.UtilizationModel;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public final class RoundRobin {
    private final List<Datacenter> datacenterList;
    private final List<Vm> vmList;
    private CloudSimPlus simulation;

    private final List<Host> allHostList;
    private final List<Cloudlet> cloudletList;
    private final List<Cloudlet> failedTasks = new ArrayList<>();

    public static void main(String[] args) {
        new RoundRobin();
    }

    private RoundRobin() {
        System.out.println("Starting " + getClass().getSimpleName());
        simulation = new CloudSimPlus();

        this.cloudletList = new ArrayList<>(LowPower.CLOUDLETS);
        this.allHostList = new ArrayList<>(LowPower.HOSTS);
        this.datacenterList = new ArrayList<>(LowPower.DATACENTERS);
        this.vmList = new ArrayList<>(LowPower.VMS);

        for (int i = 0; i < LowPower.DATACENTERS; i++)
            this.datacenterList.add(createDatacenter());

        // DatacenterBrokerSimple is basically round robin
        final var broker = new DatacenterBrokerSimple(simulation);

        createAndSubmitVms(broker);
        createAndSubmitCloudlets(broker);

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

            final Cloudlet c = new CloudletSimple(
                    i, LowPower.CLOUDLET_LENGHT, 1)
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
            final var host = new HostSimple(LowPower.HOST_RAM, LowPower.HOST_BW, LowPower.HOST_STORAGE, peList);
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
        if (LowPower.FAILURE_RNG.eventsHappened()) {
            System.out.println("FAILED task " + task.getCloudlet().getId());
            failedTasks.add(task.getCloudlet());
            // We don't reschedule the task in round robin
        }
    }
}