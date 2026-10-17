
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa540 <clockcache_get_in_cache_async.constprop.0>:
   aa540:	mov    0x18(%rdi),%rax
   aa544:	cmp    $0x1,%rax
   aa548:	je     aa7b0 <clockcache_get_in_cache_async.constprop.0+0x270>
   aa54e:	push   %r15
   aa550:	push   %r14
   aa552:	mov    %rdi,%r14
   aa555:	push   %r13
   aa557:	push   %r12
   aa559:	push   %rbp
   aa55a:	push   %rbx
   aa55b:	sub    $0x8,%rsp
   aa55f:	test   %rax,%rax
   aa562:	je     aa5e0 <clockcache_get_in_cache_async.constprop.0+0xa0>
   aa564:	mov    0x20(%r14),%rsi
   aa568:	mov    0x60(%r14),%eax
   aa56c:	xor    %edx,%edx
   aa56e:	mov    0x58(%r14),%rdi
   aa572:	mov    0x8(%rsi),%rcx
   aa576:	mov    %rdi,%r9
   aa579:	divq   0x140(%rcx)
   aa580:	and    $0x3,%r9d
   aa584:	shl    $0x6,%edx
   aa587:	lea    (%rdx,%rax,1),%r8d
   aa58b:	mov    0x130(%rcx),%eax
   aa591:	mov    $0xffffffff,%ecx
   aa596:	mov    0x88(%rsi),%rdx
   aa59d:	imul   %r9,%rax
   aa5a1:	add    %r8,%rax
   aa5a4:	lock xadd %cx,(%rdx,%rax,2)
   aa5aa:	test   %cx,%cx
   aa5ad:	je     aac57 <clockcache_get_in_cache_async.constprop.0+0x717>
   aa5b3:	movl   $0x0,0x54(%r14)
   aa5bb:	movq   $0x1,0x18(%r14)
   aa5c3:	mov    $0x1,%eax
   aa5c8:	add    $0x8,%rsp
   aa5cc:	pop    %rbx
   aa5cd:	pop    %rbp
   aa5ce:	pop    %r12
   aa5d0:	pop    %r13
   aa5d2:	pop    %r14
   aa5d4:	pop    %r15
   aa5d6:	ret
   aa5d7:	nopw   0x0(%rax,%rax,1)
   aa5e0:	data16 lea 0x2a9c0(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   aa5e8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa5f0:	mov    0x60(%r14),%r12d
   aa5f4:	mov    0x20(%r14),%rbp
   aa5f8:	mov    %r12d,%esi
   aa5fb:	mov    %rbp,%rdi
   aa5fe:	mov    (%rax),%rax
   aa601:	mov    %rax,0x58(%r14)
   aa605:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   aa60a:	cmp    $0x1,%eax
   aa60d:	jne    aa65e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa60f:	movabs $0xcccccccccccccccd,%r13
   aa619:	mov    $0x1,%r15d
   aa61f:	mov    %r15,%rax
   aa622:	mul    %r13
   aa625:	xor    %eax,%eax
   aa627:	mov    %rdx,%rbx
   aa62a:	shr    $0x2,%rbx
   aa62e:	xchg   %ax,%ax
   aa630:	pause
   aa632:	mov    %rax,%rcx
   aa635:	add    $0x1,%rax
   aa639:	cmp    %rcx,%rbx
   aa63c:	jne    aa630 <clockcache_get_in_cache_async.constprop.0+0xf0>
   aa63e:	cmp    $0x400,%r15
   aa645:	ja     aa708 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa64b:	mov    %r12d,%esi
   aa64e:	mov    %rbp,%rdi
   aa651:	add    %r15,%r15
   aa654:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   aa659:	cmp    $0x1,%eax
   aa65c:	je     aa61f <clockcache_get_in_cache_async.constprop.0+0xdf>
   aa65e:	test   %eax,%eax
   aa660:	jne    aa5b3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa666:	mov    0x20(%r14),%rsi
   aa66a:	mov    0x60(%r14),%edi
   aa66e:	mov    0x48(%rsi),%rcx
   aa672:	mov    %rdi,%rdx
   aa675:	shl    $0x6,%rdx
   aa679:	add    %rdx,%rcx
   aa67c:	mov    %rcx,0x68(%r14)
   aa680:	mov    0x8(%rcx),%r8
   aa684:	cmp    0x28(%r14),%r8
   aa688:	jne    aa7b6 <clockcache_get_in_cache_async.constprop.0+0x276>
   aa68e:	mov    0x20(%rcx),%r9d
   aa692:	mov    0x30(%r14),%ecx
   aa696:	cmp    %ecx,%r9d
   aa699:	jne    aac2c <clockcache_get_in_cache_async.constprop.0+0x6ec>
   aa69f:	add    0x48(%rsi),%rdx
   aa6a3:	mov    0x10(%rdx),%edx
   aa6a6:	and    $0x10,%edx
   aa6a9:	mov    0x20(%r14),%rcx
   aa6ad:	jne    aa830 <clockcache_get_in_cache_async.constprop.0+0x2f0>
   aa6b3:	mov    0x68(%r14),%rax
   aa6b7:	mov    0x8(%rcx),%rsi
   aa6bb:	mov    0x14(%rax),%edi
   aa6be:	test   %edi,%edi
   aa6c0:	jne    aa730 <clockcache_get_in_cache_async.constprop.0+0x1f0>
   aa6c2:	mov    0x18(%rsi),%edx
   aa6c5:	test   %edx,%edx
   aa6c7:	je     aa6e9 <clockcache_get_in_cache_async.constprop.0+0x1a9>
   aa6c9:	mov    0x58(%r14),%rsi
   aa6cd:	mov    0x30(%r14),%edi
   aa6d1:	lea    0x0(,%rsi,8),%rdx
   aa6d9:	sub    %rsi,%rdx
   aa6dc:	lea    0x220(%rdi,%rdx,8),%rdx
   aa6e4:	addq   $0x1,(%rcx,%rdx,8)
   aa6e9:	mov    %rax,0x48(%r14)
   aa6ed:	movl   $0x1,0x54(%r14)
   aa6f5:	movq   $0x1,0x18(%r14)
   aa6fd:	jmp    aa5c3 <clockcache_get_in_cache_async.constprop.0+0x83>
   aa702:	nopw   0x0(%rax,%rax,1)
   aa708:	mov    %r12d,%esi
   aa70b:	mov    %rbp,%rdi
   aa70e:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   aa713:	cmp    $0x1,%eax
   aa716:	jne    aa65e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa71c:	xor    %eax,%eax
   aa71e:	xchg   %ax,%ax
   aa720:	pause
   aa722:	mov    %rax,%rdx
   aa725:	add    $0x1,%rax
   aa729:	cmp    %rdx,%rbx
   aa72c:	jne    aa720 <clockcache_get_in_cache_async.constprop.0+0x1e0>
   aa72e:	jmp    aa708 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa730:	mov    0x14(%rax),%eax
   aa733:	xor    %edx,%edx
   aa735:	mov    0x58(%r14),%rdi
   aa739:	mov    %eax,0x50(%r14)
   aa73d:	mov    0x60(%r14),%eax
   aa741:	mov    %rdi,%r9
   aa744:	and    $0x3,%r9d
   aa748:	divq   0x140(%rsi)
   aa74f:	shl    $0x6,%edx
   aa752:	lea    (%rdx,%rax,1),%r8d
   aa756:	mov    0x130(%rsi),%eax
   aa75c:	mov    0x88(%rcx),%rdx
   aa763:	mov    $0xffffffff,%ecx
   aa768:	imul   %r9,%rax
   aa76c:	add    %r8,%rax
   aa76f:	lock xadd %cx,(%rdx,%rax,2)
   aa775:	test   %cx,%cx
   aa778:	jne    aa6ed <clockcache_get_in_cache_async.constprop.0+0x1ad>
   aa77e:	push   %rcx
   aa77f:	lea    0x182aa(%rip),%rdx        # c2a30 <__func__.28.lto_priv.1>
   aa786:	mov    $0x288,%esi
   aa78b:	xor    %eax,%eax
   aa78d:	push   %r9
   aa78f:	lea    0x6e98(%rip),%rcx        # b162e <_fini+0xa12>
   aa796:	xor    %r9d,%r9d
   aa799:	push   %r8
   aa79b:	lea    0xda56(%rip),%r8        # b81f8 <__func__.21+0xd68>
   aa7a2:	push   %rdi
   aa7a3:	lea    0x6e2d(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa7aa:	call   ee00 <platform_assert_false@plt>
   aa7af:	nop
   aa7b0:	mov    $0x1,%eax
   aa7b5:	ret
   aa7b6:	mov    0x8(%rsi),%rcx
   aa7ba:	mov    %rdi,%rax
   aa7bd:	xor    %edx,%edx
   aa7bf:	mov    0x58(%r14),%r8
   aa7c3:	divq   0x140(%rcx)
   aa7ca:	mov    %r8,%r9
   aa7cd:	and    $0x3,%r9d
   aa7d1:	shl    $0x6,%edx
   aa7d4:	lea    (%rdx,%rax,1),%edi
   aa7d7:	mov    0x130(%rcx),%eax
   aa7dd:	mov    0x88(%rsi),%rdx
   aa7e4:	mov    $0xffffffff,%ecx
   aa7e9:	imul   %r9,%rax
   aa7ed:	add    %rdi,%rax
   aa7f0:	lock xadd %cx,(%rdx,%rax,2)
   aa7f6:	test   %cx,%cx
   aa7f9:	jne    aa5b3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa7ff:	push   %rbx
   aa800:	lea    0x6e27(%rip),%rcx        # b162e <_fini+0xa12>
   aa807:	mov    $0x288,%esi
   aa80c:	xor    %eax,%eax
   aa80e:	push   %r9
   aa810:	lea    0x18219(%rip),%rdx        # c2a30 <__func__.28.lto_priv.1>
   aa817:	xor    %r9d,%r9d
   aa81a:	push   %rdi
   aa81b:	lea    0x6db5(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa822:	push   %r8
   aa824:	lea    0xd9cd(%rip),%r8        # b81f8 <__func__.21+0xd68>
   aa82b:	call   ee00 <platform_assert_false@plt>
   aa830:	mov    0x60(%r14),%edx
   aa834:	shl    $0x6,%rdx
   aa838:	add    0x48(%rcx),%rdx
   aa83c:	mov    0x10(%rdx),%edx
   aa83f:	and    $0x10,%edx
   aa842:	je     aa564 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa848:	mov    0x68(%r14),%rdx
   aa84c:	mov    $0x1,%esi
   aa851:	jmp    aa862 <clockcache_get_in_cache_async.constprop.0+0x322>
   aa853:	data16 cs nopw 0x0(%rax,%rax,1)
   aa85e:	xchg   %ax,%ax
   aa860:	pause
   aa862:	mov    %rsi,%rcx
   aa865:	xchg   %rcx,0x28(%rdx)
   aa869:	test   %rcx,%rcx
   aa86c:	jne    aa860 <clockcache_get_in_cache_async.constprop.0+0x320>
   aa86e:	mov    0x68(%r14),%rcx
   aa872:	mov    0x40(%r14),%rdx
   aa876:	lea    0x488(%r14),%r9
   aa87d:	mov    0x38(%r14),%rsi
   aa881:	lea    0x28(%rcx),%rdi
   aa885:	movq   %rdx,%xmm0
   aa88a:	movq   %rdi,%xmm1
   aa88f:	punpcklqdq %xmm1,%xmm0
   aa893:	test   %rsi,%rsi
   aa896:	je     aac04 <clockcache_get_in_cache_async.constprop.0+0x6c4>
   aa89c:	test   %rdx,%rdx
   aa89f:	je     aabdc <clockcache_get_in_cache_async.constprop.0+0x69c>
   aa8a5:	movabs $0x4153594e43575144,%rdx
   aa8af:	cmp    %rdx,0x4a8(%r14)
   aa8b6:	je     aaad0 <clockcache_get_in_cache_async.constprop.0+0x590>
   aa8bc:	mov    %rdx,0x4a8(%r14)
   aa8c3:	movups %xmm0,0x498(%r14)
   aa8cb:	mov    0x30(%rcx),%rdx
   aa8cf:	mov    %rsi,0x490(%r14)
   aa8d6:	movq   $0x0,0x488(%r14)
   aa8e1:	test   %rdx,%rdx
   aa8e4:	je     aaa2e <clockcache_get_in_cache_async.constprop.0+0x4ee>
   aa8ea:	mov    0x38(%rcx),%rdx
   aa8ee:	mov    %r9,(%rdx)
   aa8f1:	mov    %r9,0x38(%rcx)
   aa8f5:	lock orq $0x0,(%rsp)
   aa8fb:	mov    0x20(%r14),%rsi
   aa8ff:	mov    0x60(%r14),%ecx
   aa903:	shl    $0x6,%rcx
   aa907:	add    0x48(%rsi),%rcx
   aa90b:	mov    0x10(%rcx),%esi
   aa90e:	and    $0x10,%esi
   aa911:	mov    0x68(%r14),%rcx
   aa915:	je     aa92f <clockcache_get_in_cache_async.constprop.0+0x3ef>
   aa917:	lea    -0x3ba(%rip),%rsi        # aa564 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa91e:	mov    %rsi,0x18(%r14)
   aa922:	movq   $0x0,0x28(%rcx)
   aa92a:	jmp    aa5c8 <clockcache_get_in_cache_async.constprop.0+0x88>
   aa92f:	mov    0x4a8(%r14),%r8
   aa936:	mov    0x4a0(%r14),%rsi
   aa93d:	lea    0x28(%rcx),%rax
   aa941:	movabs $0x4153594e43575144,%rdi
   aa94b:	cmp    %rdi,%r8
   aa94e:	jne    aaa8f <clockcache_get_in_cache_async.constprop.0+0x54f>
   aa954:	cmp    %rsi,%rax
   aa957:	jne    aaa65 <clockcache_get_in_cache_async.constprop.0+0x525>
   aa95d:	cmpq   $0x0,0x490(%r14)
   aa965:	je     aab50 <clockcache_get_in_cache_async.constprop.0+0x610>
   aa96b:	cmpq   $0x0,0x498(%r14)
   aa973:	je     aab28 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aa979:	test   %rdx,%rdx
   aa97c:	je     aaa0a <clockcache_get_in_cache_async.constprop.0+0x4ca>
   aa982:	mov    0x20(%rdx),%r10
   aa986:	mov    0x18(%rdx),%rdi
   aa98a:	cmp    %r8,%r10
   aa98d:	jne    aaba5 <clockcache_get_in_cache_async.constprop.0+0x665>
   aa993:	cmp    %rax,%rdi
   aa996:	jne    aab78 <clockcache_get_in_cache_async.constprop.0+0x638>
   aa99c:	cmpq   $0x0,0x8(%rdx)
   aa9a1:	je     aab50 <clockcache_get_in_cache_async.constprop.0+0x610>
   aa9a7:	cmpq   $0x0,0x10(%rdx)
   aa9ac:	je     aab28 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aa9b2:	cmp    (%rdx),%r9
   aa9b5:	jne    aaa3d <clockcache_get_in_cache_async.constprop.0+0x4fd>
   aa9bb:	mov    0x488(%r14),%rax
   aa9c2:	mov    %rax,(%rdx)
   aa9c5:	cmp    0x38(%rcx),%r9
   aa9c9:	je     aaa37 <clockcache_get_in_cache_async.constprop.0+0x4f7>
   aa9cb:	movabs $0x4153594e43574944,%rax
   aa9d5:	pxor   %xmm0,%xmm0
   aa9d9:	movq   $0x0,0x488(%r14)
   aa9e4:	movq   $0x0,0x490(%r14)
   aa9ef:	mov    %rax,0x4a8(%r14)
   aa9f6:	movups %xmm0,0x498(%r14)
   aa9fe:	movq   $0x0,(%rsi)
   aaa05:	jmp    aa564 <clockcache_get_in_cache_async.constprop.0+0x24>
   aaa0a:	cmp    0x30(%rcx),%r9
   aaa0e:	jne    aab00 <clockcache_get_in_cache_async.constprop.0+0x5c0>
   aaa14:	mov    0x488(%r14),%rax
   aaa1b:	mov    %rax,0x30(%rcx)
   aaa1f:	test   %rax,%rax
   aaa22:	jne    aa9cb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aaa24:	movq   $0x0,0x38(%rcx)
   aaa2c:	jmp    aa9cb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aaa2e:	mov    %r9,0x30(%rcx)
   aaa32:	jmp    aa8f1 <clockcache_get_in_cache_async.constprop.0+0x3b1>
   aaa37:	mov    %rdx,0x38(%rcx)
   aaa3b:	jmp    aa9cb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aaa3d:	lea    0x6b36(%rip),%r8        # b157a <_fini+0x95e>
   aaa44:	lea    0x8788(%rip),%rcx        # b31d3 <_fini+0x25b7>
   aaa4b:	xor    %eax,%eax
   aaa4d:	mov    $0x17a,%esi
   aaa52:	lea    0x1ab57(%rip),%rdx        # c55b0 <__func__.12>
   aaa59:	lea    0x6c42(%rip),%rdi        # b16a2 <_fini+0xa86>
   aaa60:	call   ee00 <platform_assert_false@plt>
   aaa65:	push   %rax
   aaa66:	lea    0xde23(%rip),%r8        # b8890 <__func__.21+0x1400>
   aaa6d:	xor    %eax,%eax
   aaa6f:	lea    0x6c47(%rip),%rcx        # b16bd <_fini+0xaa1>
   aaa76:	push   %rsi
   aaa77:	lea    0x17d92(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aaa7e:	mov    $0x163,%esi
   aaa83:	lea    0x6c18(%rip),%rdi        # b16a2 <_fini+0xa86>
   aaa8a:	call   ee00 <platform_assert_false@plt>
   aaa8f:	push   0x498(%r14)
   aaa96:	lea    0xddc3(%rip),%rcx        # b8860 <__func__.21+0x13d0>
   aaa9d:	lea    0x17d6c(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aaaa4:	push   0x490(%r14)
   aaaab:	lea    0x6bf0(%rip),%rdi        # b16a2 <_fini+0xa86>
   aaab2:	push   0x488(%r14)
   aaab9:	push   %rax
   aaaba:	xor    %eax,%eax
   aaabc:	push   %rsi
   aaabd:	mov    $0x159,%esi
   aaac2:	push   %r8
   aaac4:	lea    0xdd2d(%rip),%r8        # b87f8 <__func__.21+0x1368>
   aaacb:	call   ee00 <platform_assert_false@plt>
   aaad0:	push   %rdi
   aaad1:	lea    0x11448(%rip),%r8        # bbf20 <__func__.21+0x4a90>
   aaad8:	mov    $0x13d,%esi
   aaadd:	xor    %eax,%eax
   aaadf:	push   0x4a0(%r14)
   aaae6:	lea    0x1146b(%rip),%rcx        # bbf58 <__func__.21+0x4ac8>
   aaaed:	lea    0x1aadc(%rip),%rdx        # c55d0 <__func__.13>
   aaaf4:	lea    0x6ba7(%rip),%rdi        # b16a2 <_fini+0xa86>
   aaafb:	call   ee00 <platform_assert_false@plt>
   aab00:	lea    0x6a73(%rip),%r8        # b157a <_fini+0x95e>
   aab07:	lea    0x86da(%rip),%rcx        # b31e8 <_fini+0x25cc>
   aab0e:	xor    %eax,%eax
   aab10:	mov    $0x180,%esi
   aab15:	lea    0x1aa94(%rip),%rdx        # c55b0 <__func__.12>
   aab1c:	lea    0x6b7f(%rip),%rdi        # b16a2 <_fini+0xa86>
   aab23:	call   ee00 <platform_assert_false@plt>
   aab28:	lea    0x6a4b(%rip),%r8        # b157a <_fini+0x95e>
   aab2f:	lea    0x6bb7(%rip),%rcx        # b16ed <_fini+0xad1>
   aab36:	xor    %eax,%eax
   aab38:	mov    $0x169,%esi
   aab3d:	lea    0x17ccc(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aab44:	lea    0x6b57(%rip),%rdi        # b16a2 <_fini+0xa86>
   aab4b:	call   ee00 <platform_assert_false@plt>
   aab50:	lea    0x6a23(%rip),%r8        # b157a <_fini+0x95e>
   aab57:	lea    0x6b76(%rip),%rcx        # b16d4 <_fini+0xab8>
   aab5e:	xor    %eax,%eax
   aab60:	mov    $0x168,%esi
   aab65:	lea    0x17ca4(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aab6c:	lea    0x6b2f(%rip),%rdi        # b16a2 <_fini+0xa86>
   aab73:	call   ee00 <platform_assert_false@plt>
   aab78:	push   %rax
   aab79:	mov    %rdx,%r9
   aab7c:	lea    0xdd0d(%rip),%r8        # b8890 <__func__.21+0x1400>
   aab83:	mov    $0x163,%esi
   aab88:	push   %rdi
   aab89:	lea    0x6b2d(%rip),%rcx        # b16bd <_fini+0xaa1>
   aab90:	lea    0x17c79(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aab97:	xor    %eax,%eax
   aab99:	lea    0x6b02(%rip),%rdi        # b16a2 <_fini+0xa86>
   aaba0:	call   ee00 <platform_assert_false@plt>
   aaba5:	push   0x10(%rdx)
   aaba8:	mov    %rdx,%r9
   aabab:	lea    0xdc46(%rip),%r8        # b87f8 <__func__.21+0x1368>
   aabb2:	lea    0xdca7(%rip),%rcx        # b8860 <__func__.21+0x13d0>
   aabb9:	push   0x8(%rdx)
   aabbc:	mov    $0x159,%esi
   aabc1:	push   (%rdx)
   aabc3:	lea    0x17c46(%rip),%rdx        # c2810 <__func__.16.lto_priv.1.lto_priv.0>
   aabca:	push   %rax
   aabcb:	xor    %eax,%eax
   aabcd:	push   %rdi
   aabce:	lea    0x6acd(%rip),%rdi        # b16a2 <_fini+0xa86>
   aabd5:	push   %r10
   aabd7:	call   ee00 <platform_assert_false@plt>
   aabdc:	lea    0x6997(%rip),%r8        # b157a <_fini+0x95e>
   aabe3:	lea    0x6b0b(%rip),%rcx        # b16f5 <_fini+0xad9>
   aabea:	xor    %eax,%eax
   aabec:	mov    $0x13c,%esi
   aabf1:	lea    0x1a9d8(%rip),%rdx        # c55d0 <__func__.13>
   aabf8:	lea    0x6aa3(%rip),%rdi        # b16a2 <_fini+0xa86>
   aabff:	call   ee00 <platform_assert_false@plt>
   aac04:	lea    0x696f(%rip),%r8        # b157a <_fini+0x95e>
   aac0b:	lea    0x6aca(%rip),%rcx        # b16dc <_fini+0xac0>
   aac12:	xor    %eax,%eax
   aac14:	mov    $0x13b,%esi
   aac19:	lea    0x1a9b0(%rip),%rdx        # c55d0 <__func__.13>
   aac20:	lea    0x6a7b(%rip),%rdi        # b16a2 <_fini+0xa86>
   aac27:	call   ee00 <platform_assert_false@plt>
   aac2c:	push   %r11
   aac2e:	lea    0x168ab(%rip),%r8        # c14e0 <__func__.21+0xa050>
   aac35:	mov    $0xa0e,%esi
   aac3a:	xor    %eax,%eax
   aac3c:	push   %rcx
   aac3d:	lea    0x1a9ac(%rip),%rdx        # c55f0 <__func__.14>
   aac44:	lea    0x168bd(%rip),%rcx        # c1508 <__func__.21+0xa078>
   aac4b:	lea    0x6985(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aac52:	call   ee00 <platform_assert_false@plt>
   aac57:	push   %r10
   aac59:	lea    0x69ce(%rip),%rcx        # b162e <_fini+0xa12>
   aac60:	mov    $0x288,%esi
   aac65:	xor    %eax,%eax
   aac67:	push   %r9
   aac69:	lea    0x17dc0(%rip),%rdx        # c2a30 <__func__.28.lto_priv.1>
   aac70:	xor    %r9d,%r9d
   aac73:	push   %r8
   aac75:	lea    0xd57c(%rip),%r8        # b81f8 <__func__.21+0xd68>
   aac7c:	push   %rdi
   aac7d:	lea    0x6953(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aac84:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
