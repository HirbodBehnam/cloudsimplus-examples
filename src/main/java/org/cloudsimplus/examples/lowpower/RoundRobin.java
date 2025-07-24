package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
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
    private final RoundRobinDatacenterAllocator broker;

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
        broker = new RoundRobinDatacenterAllocator(simulation);

        LowPower.createAndSubmitVms(broker, vmList);
        LowPower.createCloudlets(cloudletList, this::taskFinishedCallback);
        // We must at least submit one cloudlet apparently
        broker.submitCloudlet(cloudletList.get(0));

        simulation.startSync();
        long currentTime = 0;
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

    /**
     * This is called on every tick of the simulation
     * We will update the priority of each task in some ticks.
     * It will also submit the tasks which arrive.
     */
    private void simulationTick(long currentTime) {
        for (Cloudlet task : cloudletList) {
            if (currentTime == ((LowPower.CloudletDedline) task).getArrivalTime()) {
                this.broker.submitCloudlet(task);
            }
        }
    }
}