
.cache/invalid-address-perf-20261017/builds/outlined/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000050100 <clockcache_page_valid_for_load.lto_priv.0>:
   50100:	push   %r13
   50102:	push   %r12
   50104:	push   %rbp
   50105:	push   %rbx
   50106:	mov    %rsi,%rbx
   50109:	sub    $0x8,%rsp
   5010d:	mov    0x10(%rdi),%rbp
   50111:	mov    0x0(%rbp),%rax
   50115:	mov    %rbp,%rdi
   50118:	call   *(%rax)
   5011a:	xor    %edx,%edx
   5011c:	mov    %rax,%rcx
   5011f:	mov    (%rax),%rax
   50122:	mov    0x8(%rax),%r9
   50126:	mov    %rbx,%rax
   50129:	div    %r9
   5012c:	test   %rdx,%rdx
   5012f:	jne    501c0 <clockcache_page_valid_for_load.lto_priv.0+0xc0>
   50135:	mov    0x0(%rbp),%rax
   50139:	mov    %rbx,%r12
   5013c:	and    0x20(%rcx),%r12
   50140:	je     50170 <clockcache_page_valid_for_load.lto_priv.0+0x70>
   50142:	cmp    0x8(%rcx),%rbx
   50146:	jae    50170 <clockcache_page_valid_for_load.lto_priv.0+0x70>
   50148:	mov    %rbp,%rdi
   5014b:	mov    %r12,%rsi
   5014e:	call   *0x20(%rax)
   50151:	mov    %eax,%ebp
   50153:	test   %eax,%eax
   50155:	je     501f8 <clockcache_page_valid_for_load.lto_priv.0+0xf8>
   5015b:	xor    %eax,%eax
   5015d:	test   %ebp,%ebp
   5015f:	setne  %al
   50162:	add    $0x8,%rsp
   50166:	pop    %rbx
   50167:	pop    %rbp
   50168:	pop    %r12
   5016a:	pop    %r13
   5016c:	ret
   5016d:	nopl   (%rax)
   50170:	mov    %rbp,%rdi
   50173:	call   *0x58(%rax)
   50176:	mov    0x84e53(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   5017d:	mov    %rbx,%r9
   50180:	mov    %r12,%r8
   50183:	sub    $0x8,%rsp
   50187:	lea    0x72922(%rip),%rdx        # c2ab0 <__func__.22.lto_priv.0>
   5018e:	mov    $0x17d,%ecx
   50193:	mov    0x0(%rbp),%rdi
   50197:	push   %rax
   50198:	lea    0x673c1(%rip),%rsi        # b7560 <__func__.21+0xd0>
   5019f:	xor    %eax,%eax
   501a1:	call   fbf0 <fprintf@plt>
   501a6:	mov    0x0(%rbp),%rdi
   501aa:	call   fe90 <fflush@plt>
   501af:	pop    %rax
   501b0:	pop    %rdx
   501b1:	add    $0x8,%rsp
   501b5:	xor    %eax,%eax
   501b7:	pop    %rbx
   501b8:	pop    %rbp
   501b9:	pop    %r12
   501bb:	pop    %r13
   501bd:	ret
   501be:	xchg   %ax,%ax
   501c0:	mov    0x84e09(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   501c7:	mov    %rbx,%r8
   501ca:	xor    %eax,%eax
   501cc:	mov    $0x164,%ecx
   501d1:	lea    0x728d8(%rip),%rdx        # c2ab0 <__func__.22.lto_priv.0>
   501d8:	lea    0x672c9(%rip),%rsi        # b74a8 <__func__.21+0x18>
   501df:	mov    0x0(%rbp),%rdi
   501e3:	call   fbf0 <fprintf@plt>
   501e8:	mov    0x0(%rbp),%rdi
   501ec:	call   fe90 <fflush@plt>
   501f1:	jmp    501b1 <clockcache_page_valid_for_load.lto_priv.0+0xb1>
   501f3:	nopl   0x0(%rax,%rax,1)
   501f8:	mov    0x84dd1(%rip),%r13        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   501ff:	sub    $0x8,%rsp
   50203:	mov    %rbx,%r9
   50206:	xor    %eax,%eax
   50208:	mov    $0x172,%ecx
   5020d:	lea    0x672e4(%rip),%rsi        # b74f8 <__func__.21+0x68>
   50214:	mov    %r12,%r8
   50217:	mov    0x0(%r13),%rdi
   5021b:	push   $0x0
   5021d:	lea    0x7288c(%rip),%rdx        # c2ab0 <__func__.22.lto_priv.0>
   50224:	call   fbf0 <fprintf@plt>
   50229:	mov    0x0(%r13),%rdi
   5022d:	call   fe90 <fflush@plt>
   50232:	pop    %rcx
   50233:	pop    %rsi
   50234:	jmp    5015b <clockcache_page_valid_for_load.lto_priv.0+0x5b>

Disassembly of section .fini:
