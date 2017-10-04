#include <dax.h>

#pragma D option quiet

#define PRINT_COMMAND(i)                                     \
	printf("%10s %7d %9d %9d %5d %5d %5d %5d %5d %5d %3d\n",\
			Cmd[i], Count[i], Elements[i], Events[i].cycles,    \
			Events[i].page, Events[i].split, Events[i].unzip,   \
			Events[i].copy, Events[i].retry, Events[i].nomap,   \
			Events[i].emulate)

#define END_COMMAND(cmd)             \
	dtrace:::END                    \
/ Count[cmd] != 0 /             \
{                               \
	PRINT_COMMAND(cmd);     \
}

dax_perf_event_t Events[10];
long Elements[10];
long Count[10];

long PostCount;

dtrace:::BEGIN
{
	Cmd[DAX_CMD_SCAN_VALUE] = "scan_value";
	Cmd[DAX_CMD_SCAN_RANGE] = "scan_range";
	Cmd[DAX_CMD_TRANSLATE] = "translate";
	Cmd[DAX_CMD_SELECT] = "select";
	Cmd[DAX_CMD_EXTRACT] = "extract";
	Cmd[DAX_CMD_COPY] = "copy";
	Cmd[DAX_CMD_FILL] = "fill";
	Cmd[DAX_CMD_AND] = "and";
	Cmd[DAX_CMD_OR] = "or";
	Cmd[DAX_CMD_XOR] = "xor";

	PostCount = 0;
}

dax$target:::dax-post
{
	ctx = (dax_context_t *) copyin(arg0, sizeof(dax_context_t));
	queue = (dax_queue_t *) copyin(arg1, sizeof(dax_queue_t));

	PostCount++;
}

fbt:dax:dax_collect_dax_kstats:entry
{
}

dax$target:::dax-execute,
	dax$target:::dax-poll
{
	cmd = DAX_DFILTER_CMD(arg0);
	req = (dax_request_t *) copyin(arg1, sizeof(dax_request_t));
	res = (dax_result_t *) copyin(arg2, sizeof(dax_result_t));
	ev = (dax_perf_event_t *) copyin(arg3, sizeof(dax_perf_event_t));
	Count[cmd]++;
	elements = (cmd == DAX_CMD_COPY ? req->arg.copy.count :
			(cmd == DAX_CMD_FILL ? req->arg.fill.count :
			 req->src.elements));
	Elements[cmd] += elements;
	Events[cmd].frequency = ev->frequency;
	Events[cmd].cycles += ev->cycles;
	Events[cmd].page += ev->page;
	Events[cmd].emulate += ev->emulate;
	Events[cmd].nomap += ev->nomap;
	Events[cmd].copy += ev->copy;
	Events[cmd].retry += ev->retry;
	Events[cmd].split += ev->split;
	Events[cmd].unzip += ev->unzip;
}

fbt:dax::entry
{
	@aggr[probefunc]=count();
}

fbt:dax::entry
{
	self->t=timestamp;
}
fbt:dax::return
/self->t!=0/
{
	@times[ustack(100,500),probemod,probefunc]=quantize(timestamp-self->t);
	self->t=0;
}
tick-1s
{
	trunc(@times,10);
	printa(@times);
	trunc(@times);
}

END
{
	printa(@times);
	trunc(@times);
}

fbt:dax:dax_open:entry
{
	printf("Opened DAX!\n");
	printf("%s, %s, %s, %s \n", probeprov, probemod, probefunc, probename);
	trace(tid);
	printf("\n");
	trace(arg0)
}

fbt:dax:dax_close:return
{
	printf("Closed DAX!\n");
}

dtrace:::END
{
	printa(@aggr);
	printf("\nNumber of posts: %ld \n\n", PostCount);
	printf("%10s %7s %9s %9s %5s %5s %5s %5s %5s %5s %3s\n",
			"command", "count", "elems", "cycles", "cross", "split",
			"unzip", "copy", "retry", "nomap", "em");

}

	END_COMMAND(DAX_CMD_SCAN_VALUE)
	END_COMMAND(DAX_CMD_SCAN_RANGE)
	END_COMMAND(DAX_CMD_TRANSLATE)
	END_COMMAND(DAX_CMD_SELECT)
	END_COMMAND(DAX_CMD_EXTRACT)
	END_COMMAND(DAX_CMD_COPY)
	END_COMMAND(DAX_CMD_FILL)
	END_COMMAND(DAX_CMD_AND)
	END_COMMAND(DAX_CMD_OR)
END_COMMAND(DAX_CMD_XOR)
