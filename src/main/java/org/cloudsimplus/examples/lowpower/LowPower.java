package org.cloudsimplus.examples.lowpower;

import java.util.List;
import java.util.Random;

import org.cloudsimplus.distributions.PoissonDistr;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.vms.HostResourceStats;

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
    public static final int VMS = HOSTS * DATACENTERS;
    public static final int CLOUDLETS = 500;
    public static final int SCHEDULE_TIME_TO_PROCESS_DATACENTER_EVENTS = 5;

    public static final Random rng = new Random();

    public static final double MTTF = 8640;
    public static final PoissonDistr FAILURE_RNG = new PoissonDistr(1 / MTTF);

    /**
     * The cycle which we renew the priority of each task
     */
    public static final double T_p = 100;

    public static void printHostsCpuUtilizationAndPowerConsumption(final List<Host> hostList) {
        System.out.println("Host Power Consumptions:");
        for (final Host host : hostList) {
            final HostResourceStats cpuStats = host.getCpuUtilizationStats();

            // The total Host's CPU utilization for the time specified by the map key
            final double utilizationPercentMean = cpuStats.getMean();
            final double watts = host.getPowerModel().getPower(utilizationPercentMean);
            System.out.printf(
                    "Host %2d CPU Usage mean: %6.1f%% | Power Consumption mean: %8.0f W%n",
                    host.getId(), utilizationPercentMean * 100, watts);
        }
        System.out.println();
    }
}
