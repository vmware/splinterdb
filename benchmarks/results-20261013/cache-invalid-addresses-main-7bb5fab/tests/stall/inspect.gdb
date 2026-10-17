set pagination off
python
import gdb, json, collections, struct
inf=gdb.selected_inferior()
cc=None
windows=[]
for t in inf.threads():
 t.switch()
 f=gdb.newest_frame()
 record={'thread':t.ptid[1]}
 while f:
  name=f.name() or ''
  if name=='clockcache_get_free_page':
   cc=f.read_var('cc')
   record['requested_type']=str(f.read_var('type'))
  if name=='clockcache_acquire_entry_for_load':
   requested=int(f.read_var('addr'))
   cache=f.read_var('cc')
   slot=int(cache['lookup'][requested>>int(cache['cfg']['log_page_size'])])
   record.update(requested_addr=requested,current_mapping=slot)
  if name=='test_do_read_batch':
   record.update(batch_start=int(f.read_var('batch_start')),j=int(f.read_var('j')))
  if name=='test_reader_thread':
   params=f.read_var('params')
   record.update(tid=int(f.read_var('tid')),i=int(f.read_var('i')),k=int(f.read_var('k')),working_set_pages=int(params['num_pages_ws']))
   handles=inf.read_memory(int(params['handle_arr']),gdb.lookup_type('page_handle').sizeof*int(params['num_pages'])).tobytes()
   stride_handle=gdb.lookup_type('page_handle').sizeof
   record['owned_completed_handles']=sum(struct.unpack_from('<Q',handles,z)[0]!=0 for z in range(0,len(handles),stride_handle))
   record['async_context_statuses']=dict(collections.Counter(str(params['ctxt'][z]['status']) for z in range(32)))
  f=f.older()
 if 'tid' in record: windows.append(record)
assert cc is not None
n=int(cc['cfg']['page_capacity']); lines=int(cc['cfg']['cacheline_capacity'])
entry_type=gdb.lookup_type('clockcache_entry')
stride=entry_type.sizeof
status_offset=next(f.bitpos//8 for f in entry_type.fields() if f.name=='status')
pages=inf.read_memory(int(cc['entry']),stride*n).tobytes()
refs=struct.unpack('<'+'H'*(4*n),inf.read_memory(int(cc['refcount']),2*4*n).tobytes())
pins=inf.read_memory(int(cc['pincount']),n).tobytes()
statuses=collections.Counter(); reference_counts=collections.Counter(); held=free=loading=pinned=unreferenced=0
for i in range(n):
 status=struct.unpack_from('<I',pages,i*stride+status_offset)[0]
 rc_idx=i%lines*64+i//lines
 count=sum(refs[j*n+rc_idx] for j in range(4))
 statuses[status]+=1;reference_counts[count]+=1
 held+=count>0;free+=bool(status&1);loading+=bool(status&16);pinned+=pins[i]>0;unreferenced+=count==0
busy=sum(int(cc['batches'][i]['busy'])!=0 for i in range(int(cc['cfg']['batch_capacity'])))
print(json.dumps(dict(capacity=n,held_pages=held,total_references=sum(refs),free=free,unreferenced=unreferenced,loading=loading,pinned=pinned,busy_batches=busy,status_counts=dict(statuses),reference_counts=dict(reference_counts),windows=sorted(windows,key=lambda x:x['tid'])),indent=2))
end
