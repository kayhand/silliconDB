#pragma D option quiet
#pragma D option bufsize=64k

dtrace:::BEGIN
{
	printf("%4s %4s %4s %4s %6s  %-16s %s\n",
			"CPU", "CHIP", "PSET", "LGRP", "CLOCK", "TYPE", "FPU");
	done[0] = 0;
}

profile:::profile-10ms
/done[cpu] == 0/
{
	printf("%4d %4d %4d %4d %6d  %-16s %s\n",
			cpu, curcpu->cpu_chip, curcpu->cpu_pset,
			curcpu->cpu_lgrp, curcpu->cpu_info.pi_clock,
			stringof(curcpu->cpu_info.pi_processor_type),
			stringof(curcpu->cpu_info.pi_fputypes));
	done[cpu]++;
}

profile:::tick-100ms
{
	exit(0);
}
