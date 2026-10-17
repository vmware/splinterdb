
.cache/invalid-address-perf-20261017/builds/mask-original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa4b0 <clockcache_try_get_read.constprop.0>:
   aa4b0:	push   %rbp
   aa4b1:	mov    %esi,%ebp
   aa4b3:	push   %rbx
   aa4b4:	mov    %rdi,%rbx
   aa4b7:	sub    $0x8,%rsp
   aa4bb:	data16 lea 0x2bae5(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   aa4c3:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa4cb:	mov    0x48(%rbx),%r10
   aa4cf:	mov    %ebp,%ecx
   aa4d1:	mov    %rcx,%r9
   aa4d4:	shl    $0x6,%r9
   aa4d8:	add    %r9,%r10
   aa4db:	mov    (%rax),%rdi
   aa4de:	mov    0x10(%r10),%eax
   aa4e2:	test   $0x20,%al
   aa4e4:	jne    aa558 <clockcache_try_get_read.constprop.0+0xa8>
   aa4e6:	mov    0x8(%rbx),%rsi
   aa4ea:	mov    %rcx,%rax
   aa4ed:	xor    %edx,%edx
   aa4ef:	mov    %rdi,%r8
   aa4f2:	and    $0x3,%r8d
   aa4f6:	divq   0x140(%rsi)
   aa4fd:	shl    $0x6,%edx
   aa500:	add    %eax,%edx
   aa502:	mov    0x130(%rsi),%eax
   aa508:	mov    %edx,%edx
   aa50a:	imul   %r8,%rax
   aa50e:	add    %rax,%rdx
   aa511:	mov    0x88(%rbx),%rax
   aa518:	lock addw $0x1,(%rax,%rdx,2)
   aa51e:	je     aa5ae <clockcache_try_get_read.constprop.0+0xfe>
   aa524:	mov    0x10(%r10),%esi
   aa528:	mov    %esi,%eax
   aa52a:	and    $0x21,%eax
   aa52d:	jne    aa568 <clockcache_try_get_read.constprop.0+0xb8>
   aa52f:	and    $0x2,%esi
   aa532:	je     aa540 <clockcache_try_get_read.constprop.0+0x90>
   aa534:	add    $0x8,%rsp
   aa538:	pop    %rbx
   aa539:	pop    %rbp
   aa53a:	ret
   aa53b:	nopl   0x0(%rax,%rax,1)
   aa540:	mov    0x48(%rbx),%rdx
   aa544:	lock orl $0x2,0x10(%rdx,%r9,1)
   aa54b:	add    $0x8,%rsp
   aa54f:	pop    %rbx
   aa550:	pop    %rbp
   aa551:	ret
   aa552:	nopw   0x0(%rax,%rax,1)
   aa558:	add    $0x8,%rsp
   aa55c:	mov    $0x1,%eax
   aa561:	pop    %rbx
   aa562:	pop    %rbp
   aa563:	ret
   aa564:	nopl   0x0(%rax)
   aa568:	mov    0x8(%rbx),%r9
   aa56c:	mov    %rcx,%rax
   aa56f:	xor    %edx,%edx
   aa571:	divq   0x140(%r9)
   aa578:	shl    $0x6,%edx
   aa57b:	lea    (%rdx,%rax,1),%ecx
   aa57e:	mov    0x130(%r9),%eax
   aa585:	mov    $0xffffffff,%r9d
   aa58b:	mov    0x88(%rbx),%rdx
   aa592:	imul   %r8,%rax
   aa596:	add    %rcx,%rax
   aa599:	lock xadd %r9w,(%rdx,%rax,2)
   aa5a0:	test   %r9w,%r9w
   aa5a4:	je     aa5dc <clockcache_try_get_read.constprop.0+0x12c>
   aa5a6:	and    $0x1,%esi
   aa5a9:	lea    0x1(%rsi),%eax
   aa5ac:	jmp    aa534 <clockcache_try_get_read.constprop.0+0x84>
   aa5ae:	push   %rdx
   aa5af:	mov    %ebp,%r9d
   aa5b2:	lea    0x80ba(%rip),%rcx        # b2673 <_fini+0x1597>
   aa5b9:	mov    $0x25e,%esi
   aa5be:	push   %r8
   aa5c0:	lea    0x19449(%rip),%rdx        # c3a10 <__func__.30.lto_priv.1>
   aa5c7:	lea    0xeeca(%rip),%r8        # b9498 <__func__.21+0x1008>
   aa5ce:	xor    %eax,%eax
   aa5d0:	lea    0x8000(%rip),%rdi        # b25d7 <_fini+0x14fb>
   aa5d7:	call   ee00 <platform_assert_false@plt>
   aa5dc:	push   %rax
   aa5dd:	xor    %r9d,%r9d
   aa5e0:	lea    0x19449(%rip),%rdx        # c3a30 <__func__.28.lto_priv.1>
   aa5e7:	mov    $0x27c,%esi
   aa5ec:	push   %r8
   aa5ee:	xor    %eax,%eax
   aa5f0:	lea    0xeb91(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa5f7:	push   %rcx
   aa5f8:	lea    0x802f(%rip),%rcx        # b262e <_fini+0x1552>
   aa5ff:	push   %rdi
   aa600:	lea    0x7fd0(%rip),%rdi        # b25d7 <_fini+0x14fb>
   aa607:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
