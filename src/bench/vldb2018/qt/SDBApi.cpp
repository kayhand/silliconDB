#include "SDBApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

#ifdef __sun 
void SDBApi::hwScan(dax_queue_t **queue, Node<Query>* node)
{ 
    //int curPart = scaledPart % this->num_of_parts; //*
    int curPart = node->value.getPart();
    int ind = selCol + (curPart) * this->num_of_columns;
    column *col = &(this->compTable->columns[ind]);
    int num_of_els = col->c_meta.col_size;

    this->src.data = col->compressed;
    this->src.elements = num_of_els;
    this->dst.elements = num_of_els;

    //Move this into the API
    void* bit_vector = this->dataComp->getFilterBitVector(curPart * this->num_of_segments);
    dst.data = bit_vector;//*
    printf("Dax will scan part (%d) for table (%d) -- \n bit vector start: %d \n", curPart, node->value.getTableId(), curPart * this->num_of_segments);

    node->t_start = gethrtime();
    node->post_data.t_start = gethrtime();
    node->post_data.node_ptr = (void *) node;

    //dax_status_t scan_status = dax_scan_value_post(*queue, this->flag, &(this->src), &(this->dst), this->cmp, &(this->predicate), (void *) node);
    dax_status_t scan_status = dax_scan_value_post(*queue, this->flag, &(this->src), &(this->dst), this->cmp, &(this->predicate), (void *) &(node->post_data));
    printf("Dax (%d), (%d)\n", curPart, node->value.getTableId());
    if(scan_status != 0)
        printf("Dax Error! %d\n", scan_status);

} 

#endif
