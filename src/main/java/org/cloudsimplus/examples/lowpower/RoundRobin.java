package org.cloudsimplus.examples.lowpower;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
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
import java.util.List;

/**
 * 
 */
public final class RoundRobin {
    private final List<Datacenter> datacenterList;
    private final List<Vm> vmList = new ArrayList<>(LowPower.VMS);
    private CloudSimPlus simulation;

    /**
     * The file containing the Customer's SLA Contract in JSON format.
     */
    private final SlaContract contract;
    private final List<Cloudlet> cloudletList;

    public static void main(String[] args) {
        new RoundRobin();
    }

    private RoundRobin() {
        /*
         * Enables just some level of log messages.
         * Make sure to import org.cloudsimplus.util.Log;
         */
        // Log.setLevel(ch.qos.logback.classic.Level.WARN);

        System.out.println("Starting " + getClass().getSimpleName());
        simulation = new CloudSimPlus();

        this.contract = SlaContract.getInstance(LowPower.CUSTOMER_SLA_CONTRACT);
        cloudletList = new ArrayList<>(LowPower.CLOUDLETS);
        this.datacenterList = new ArrayList<>(LowPower.DATACENTERS);

        for (int i = 0; i < LowPower.DATACENTERS; i++)
            this.datacenterList.add(createDatacenter());

        // DatacenterBrokerSimple is basically round robin
        final var broker = new DatacenterBrokerSimple(simulation);

        createAndSubmitVms(broker);
        createAndSubmitCloudlets(broker);

        simulation.start();

        new CloudletsTableBuilder(broker.getCloudletFinishedList()).build();

        System.out.println(getClass().getSimpleName() + " finished!");
    }

    private void createAndSubmitCloudlets(final DatacenterBroker broker) {
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
            cloudletList.add(c);
        }

        broker.submitCloudletList(cloudletList);
    }

    private void createAndSubmitVms(final DatacenterBroker broker) {
        for (int i = 0; i < LowPower.VMS; i++) {
            Vm vm = createVm();
            vmList.add(vm);
        }
        broker.submitVmList(vmList);
    }

    private Vm createVm() {
        final Vm vm = new VmSimple(vmList.size(), LowPower.VM_MIPS[LowPower.rng.nextInt(LowPower.VM_MIPS.length)],
                LowPower.VM_PES_NUM);
        vm.setRam(LowPower.VM_RAM).setBw(LowPower.VM_BW).setSize(LowPower.VM_SIZE)
                .setCloudletScheduler(new CloudletSchedulerTimeShared())
                .enableUtilizationStats();
        return vm;
    }

    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(LowPower.HOSTS);
        for (int i = 0; i < LowPower.HOSTS; i++) {
            hostList.add(createHost(LowPower.HOST_NUMBER_OF_PES, LowPower.HOST_MIPS_BY_PE));
        }

        return new DatacenterSimple(simulation, hostList);
    }

    private Host createHost(final int pesNumber, final long mipsByPe) {
        final var peList = createPeList(pesNumber, mipsByPe);
        final var host = new HostSimple(LowPower.HOST_RAM, LowPower.HOST_BW, LowPower.HOST_STORAGE, peList);
        host.setPowerModel(new PowerModelHostSimple(1000, 700));
        host.setRamProvisioner(new ResourceProvisionerSimple());
        host.setBwProvisioner(new ResourceProvisionerSimple());
        host.setVmScheduler(new VmSchedulerTimeShared());
        return host;
    }

    private List<Pe> createPeList(final int numberOfPEs, final long mips) {
        final var peList = new ArrayList<Pe>(numberOfPEs);
        for (int i = 0; i < numberOfPEs; i++) {
            peList.add(new PeSimple(mips));
        }
        return peList;
    }
}