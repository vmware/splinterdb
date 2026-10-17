
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa4f0 <clockcache_try_get_read.constprop.0>:
   aa4f0:	push   %rbp
   aa4f1:	mov    %esi,%ebp
   aa4f3:	push   %rbx
   aa4f4:	mov    %rdi,%rbx
   aa4f7:	sub    $0x8,%rsp
   aa4fb:	data16 lea 0x2baa5(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   aa503:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa50b:	mov    0x48(%rbx),%r10
   aa50f:	mov    %ebp,%ecx
   aa511:	mov    %rcx,%r9
   aa514:	shl    $0x6,%r9
   aa518:	add    %r9,%r10
   aa51b:	mov    (%rax),%rdi
   aa51e:	mov    0x10(%r10),%eax
   aa522:	test   $0x20,%al
   aa524:	jne    aa598 <clockcache_try_get_read.constprop.0+0xa8>
   aa526:	mov    0x8(%rbx),%rsi
   aa52a:	mov    %rcx,%rax
   aa52d:	xor    %edx,%edx
   aa52f:	mov    %rdi,%r8
   aa532:	and    $0x3,%r8d
   aa536:	divq   0x140(%rsi)
   aa53d:	shl    $0x6,%edx
   aa540:	add    %eax,%edx
   aa542:	mov    0x130(%rsi),%eax
   aa548:	mov    %edx,%edx
   aa54a:	imul   %r8,%rax
   aa54e:	add    %rax,%rdx
   aa551:	mov    0x88(%rbx),%rax
   aa558:	lock addw $0x1,(%rax,%rdx,2)
   aa55e:	je     aa5ee <clockcache_try_get_read.constprop.0+0xfe>
   aa564:	mov    0x10(%r10),%esi
   aa568:	mov    %esi,%eax
   aa56a:	and    $0x21,%eax
   aa56d:	jne    aa5a8 <clockcache_try_get_read.constprop.0+0xb8>
   aa56f:	and    $0x2,%esi
   aa572:	je     aa580 <clockcache_try_get_read.constprop.0+0x90>
   aa574:	add    $0x8,%rsp
   aa578:	pop    %rbx
   aa579:	pop    %rbp
   aa57a:	ret
   aa57b:	nopl   0x0(%rax,%rax,1)
   aa580:	mov    0x48(%rbx),%rdx
   aa584:	lock orl $0x2,0x10(%rdx,%r9,1)
   aa58b:	add    $0x8,%rsp
   aa58f:	pop    %rbx
   aa590:	pop    %rbp
   aa591:	ret
   aa592:	nopw   0x0(%rax,%rax,1)
   aa598:	add    $0x8,%rsp
   aa59c:	mov    $0x1,%eax
   aa5a1:	pop    %rbx
   aa5a2:	pop    %rbp
   aa5a3:	ret
   aa5a4:	nopl   0x0(%rax)
   aa5a8:	mov    0x8(%rbx),%r9
   aa5ac:	mov    %rcx,%rax
   aa5af:	xor    %edx,%edx
   aa5b1:	divq   0x140(%r9)
   aa5b8:	shl    $0x6,%edx
   aa5bb:	lea    (%rdx,%rax,1),%ecx
   aa5be:	mov    0x130(%r9),%eax
   aa5c5:	mov    $0xffffffff,%r9d
   aa5cb:	mov    0x88(%rbx),%rdx
   aa5d2:	imul   %r8,%rax
   aa5d6:	add    %rcx,%rax
   aa5d9:	lock xadd %r9w,(%rdx,%rax,2)
   aa5e0:	test   %r9w,%r9w
   aa5e4:	je     aa61c <clockcache_try_get_read.constprop.0+0x12c>
   aa5e6:	and    $0x1,%esi
   aa5e9:	lea    0x1(%rsi),%eax
   aa5ec:	jmp    aa574 <clockcache_try_get_read.constprop.0+0x84>
   aa5ee:	push   %rdx
   aa5ef:	mov    %ebp,%r9d
   aa5f2:	lea    0x807a(%rip),%rcx        # b2673 <_fini+0x1557>
   aa5f9:	mov    $0x25e,%esi
   aa5fe:	push   %r8
   aa600:	lea    0x193c9(%rip),%rdx        # c39d0 <__func__.30.lto_priv.1>
   aa607:	lea    0xee62(%rip),%r8        # b9470 <__func__.21+0xfe0>
   aa60e:	xor    %eax,%eax
   aa610:	lea    0x7fc0(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa617:	call   ee00 <platform_assert_false@plt>
   aa61c:	push   %rax
   aa61d:	xor    %r9d,%r9d
   aa620:	lea    0x193c9(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa627:	mov    $0x27c,%esi
   aa62c:	push   %r8
   aa62e:	xor    %eax,%eax
   aa630:	lea    0xeb51(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa637:	push   %rcx
   aa638:	lea    0x7fef(%rip),%rcx        # b262e <_fini+0x1512>
   aa63f:	push   %rdi
   aa640:	lea    0x7f90(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa647:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
