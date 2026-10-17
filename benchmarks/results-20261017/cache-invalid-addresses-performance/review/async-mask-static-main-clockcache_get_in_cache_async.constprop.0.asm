
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa480 <clockcache_get_in_cache_async.constprop.0>:
   aa480:	mov    0x18(%rdi),%rax
   aa484:	cmp    $0x1,%rax
   aa488:	je     aa6f0 <clockcache_get_in_cache_async.constprop.0+0x270>
   aa48e:	push   %r15
   aa490:	push   %r14
   aa492:	mov    %rdi,%r14
   aa495:	push   %r13
   aa497:	push   %r12
   aa499:	push   %rbp
   aa49a:	push   %rbx
   aa49b:	sub    $0x8,%rsp
   aa49f:	test   %rax,%rax
   aa4a2:	je     aa520 <clockcache_get_in_cache_async.constprop.0+0xa0>
   aa4a4:	mov    0x20(%r14),%rsi
   aa4a8:	mov    0x60(%r14),%eax
   aa4ac:	xor    %edx,%edx
   aa4ae:	mov    0x58(%r14),%rdi
   aa4b2:	mov    0x8(%rsi),%rcx
   aa4b6:	mov    %rdi,%r9
   aa4b9:	divq   0x140(%rcx)
   aa4c0:	and    $0x3,%r9d
   aa4c4:	shl    $0x6,%edx
   aa4c7:	lea    (%rdx,%rax,1),%r8d
   aa4cb:	mov    0x130(%rcx),%eax
   aa4d1:	mov    $0xffffffff,%ecx
   aa4d6:	mov    0x88(%rsi),%rdx
   aa4dd:	imul   %r9,%rax
   aa4e1:	add    %r8,%rax
   aa4e4:	lock xadd %cx,(%rdx,%rax,2)
   aa4ea:	test   %cx,%cx
   aa4ed:	je     aab97 <clockcache_get_in_cache_async.constprop.0+0x717>
   aa4f3:	movl   $0x0,0x54(%r14)
   aa4fb:	movq   $0x1,0x18(%r14)
   aa503:	mov    $0x1,%eax
   aa508:	add    $0x8,%rsp
   aa50c:	pop    %rbx
   aa50d:	pop    %rbp
   aa50e:	pop    %r12
   aa510:	pop    %r13
   aa512:	pop    %r14
   aa514:	pop    %r15
   aa516:	ret
   aa517:	nopw   0x0(%rax,%rax,1)
   aa520:	data16 lea 0x2aa80(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   aa528:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa530:	mov    0x60(%r14),%r12d
   aa534:	mov    0x20(%r14),%rbp
   aa538:	mov    %r12d,%esi
   aa53b:	mov    %rbp,%rdi
   aa53e:	mov    (%rax),%rax
   aa541:	mov    %rax,0x58(%r14)
   aa545:	call   aa320 <clockcache_try_get_read.constprop.0>
   aa54a:	cmp    $0x1,%eax
   aa54d:	jne    aa59e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa54f:	movabs $0xcccccccccccccccd,%r13
   aa559:	mov    $0x1,%r15d
   aa55f:	mov    %r15,%rax
   aa562:	mul    %r13
   aa565:	xor    %eax,%eax
   aa567:	mov    %rdx,%rbx
   aa56a:	shr    $0x2,%rbx
   aa56e:	xchg   %ax,%ax
   aa570:	pause
   aa572:	mov    %rax,%rcx
   aa575:	add    $0x1,%rax
   aa579:	cmp    %rcx,%rbx
   aa57c:	jne    aa570 <clockcache_get_in_cache_async.constprop.0+0xf0>
   aa57e:	cmp    $0x400,%r15
   aa585:	ja     aa648 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa58b:	mov    %r12d,%esi
   aa58e:	mov    %rbp,%rdi
   aa591:	add    %r15,%r15
   aa594:	call   aa320 <clockcache_try_get_read.constprop.0>
   aa599:	cmp    $0x1,%eax
   aa59c:	je     aa55f <clockcache_get_in_cache_async.constprop.0+0xdf>
   aa59e:	test   %eax,%eax
   aa5a0:	jne    aa4f3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa5a6:	mov    0x20(%r14),%rsi
   aa5aa:	mov    0x60(%r14),%edi
   aa5ae:	mov    0x48(%rsi),%rcx
   aa5b2:	mov    %rdi,%rdx
   aa5b5:	shl    $0x6,%rdx
   aa5b9:	add    %rdx,%rcx
   aa5bc:	mov    %rcx,0x68(%r14)
   aa5c0:	mov    0x8(%rcx),%r8
   aa5c4:	cmp    0x28(%r14),%r8
   aa5c8:	jne    aa6f6 <clockcache_get_in_cache_async.constprop.0+0x276>
   aa5ce:	mov    0x20(%rcx),%r9d
   aa5d2:	mov    0x30(%r14),%ecx
   aa5d6:	cmp    %ecx,%r9d
   aa5d9:	jne    aab6c <clockcache_get_in_cache_async.constprop.0+0x6ec>
   aa5df:	add    0x48(%rsi),%rdx
   aa5e3:	mov    0x10(%rdx),%edx
   aa5e6:	and    $0x10,%edx
   aa5e9:	mov    0x20(%r14),%rcx
   aa5ed:	jne    aa770 <clockcache_get_in_cache_async.constprop.0+0x2f0>
   aa5f3:	mov    0x68(%r14),%rax
   aa5f7:	mov    0x8(%rcx),%rsi
   aa5fb:	mov    0x14(%rax),%edi
   aa5fe:	test   %edi,%edi
   aa600:	jne    aa670 <clockcache_get_in_cache_async.constprop.0+0x1f0>
   aa602:	mov    0x18(%rsi),%edx
   aa605:	test   %edx,%edx
   aa607:	je     aa629 <clockcache_get_in_cache_async.constprop.0+0x1a9>
   aa609:	mov    0x58(%r14),%rsi
   aa60d:	mov    0x30(%r14),%edi
   aa611:	lea    0x0(,%rsi,8),%rdx
   aa619:	sub    %rsi,%rdx
   aa61c:	lea    0x220(%rdi,%rdx,8),%rdx
   aa624:	addq   $0x1,(%rcx,%rdx,8)
   aa629:	mov    %rax,0x48(%r14)
   aa62d:	movl   $0x1,0x54(%r14)
   aa635:	movq   $0x1,0x18(%r14)
   aa63d:	jmp    aa503 <clockcache_get_in_cache_async.constprop.0+0x83>
   aa642:	nopw   0x0(%rax,%rax,1)
   aa648:	mov    %r12d,%esi
   aa64b:	mov    %rbp,%rdi
   aa64e:	call   aa320 <clockcache_try_get_read.constprop.0>
   aa653:	cmp    $0x1,%eax
   aa656:	jne    aa59e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa65c:	xor    %eax,%eax
   aa65e:	xchg   %ax,%ax
   aa660:	pause
   aa662:	mov    %rax,%rdx
   aa665:	add    $0x1,%rax
   aa669:	cmp    %rdx,%rbx
   aa66c:	jne    aa660 <clockcache_get_in_cache_async.constprop.0+0x1e0>
   aa66e:	jmp    aa648 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa670:	mov    0x14(%rax),%eax
   aa673:	xor    %edx,%edx
   aa675:	mov    0x58(%r14),%rdi
   aa679:	mov    %eax,0x50(%r14)
   aa67d:	mov    0x60(%r14),%eax
   aa681:	mov    %rdi,%r9
   aa684:	and    $0x3,%r9d
   aa688:	divq   0x140(%rsi)
   aa68f:	shl    $0x6,%edx
   aa692:	lea    (%rdx,%rax,1),%r8d
   aa696:	mov    0x130(%rsi),%eax
   aa69c:	mov    0x88(%rcx),%rdx
   aa6a3:	mov    $0xffffffff,%ecx
   aa6a8:	imul   %r9,%rax
   aa6ac:	add    %r8,%rax
   aa6af:	lock xadd %cx,(%rdx,%rax,2)
   aa6b5:	test   %cx,%cx
   aa6b8:	jne    aa62d <clockcache_get_in_cache_async.constprop.0+0x1ad>
   aa6be:	push   %rcx
   aa6bf:	lea    0x182ca(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   aa6c6:	mov    $0x261,%esi
   aa6cb:	xor    %eax,%eax
   aa6cd:	push   %r9
   aa6cf:	lea    0x6f4e(%rip),%rcx        # b1624 <_fini+0xa48>
   aa6d6:	xor    %r9d,%r9d
   aa6d9:	push   %r8
   aa6db:	lea    0xdb0e(%rip),%r8        # b81f0 <__func__.21+0xd30>
   aa6e2:	push   %rdi
   aa6e3:	lea    0x6eed(%rip),%rdi        # b15d7 <_fini+0x9fb>
   aa6ea:	call   ee00 <platform_assert_false@plt>
   aa6ef:	nop
   aa6f0:	mov    $0x1,%eax
   aa6f5:	ret
   aa6f6:	mov    0x8(%rsi),%rcx
   aa6fa:	mov    %rdi,%rax
   aa6fd:	xor    %edx,%edx
   aa6ff:	mov    0x58(%r14),%r8
   aa703:	divq   0x140(%rcx)
   aa70a:	mov    %r8,%r9
   aa70d:	and    $0x3,%r9d
   aa711:	shl    $0x6,%edx
   aa714:	lea    (%rdx,%rax,1),%edi
   aa717:	mov    0x130(%rcx),%eax
   aa71d:	mov    0x88(%rsi),%rdx
   aa724:	mov    $0xffffffff,%ecx
   aa729:	imul   %r9,%rax
   aa72d:	add    %rdi,%rax
   aa730:	lock xadd %cx,(%rdx,%rax,2)
   aa736:	test   %cx,%cx
   aa739:	jne    aa4f3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa73f:	push   %rbx
   aa740:	lea    0x6edd(%rip),%rcx        # b1624 <_fini+0xa48>
   aa747:	mov    $0x261,%esi
   aa74c:	xor    %eax,%eax
   aa74e:	push   %r9
   aa750:	lea    0x18239(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   aa757:	xor    %r9d,%r9d
   aa75a:	push   %rdi
   aa75b:	lea    0x6e75(%rip),%rdi        # b15d7 <_fini+0x9fb>
   aa762:	push   %r8
   aa764:	lea    0xda85(%rip),%r8        # b81f0 <__func__.21+0xd30>
   aa76b:	call   ee00 <platform_assert_false@plt>
   aa770:	mov    0x60(%r14),%edx
   aa774:	shl    $0x6,%rdx
   aa778:	add    0x48(%rcx),%rdx
   aa77c:	mov    0x10(%rdx),%edx
   aa77f:	and    $0x10,%edx
   aa782:	je     aa4a4 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa788:	mov    0x68(%r14),%rdx
   aa78c:	mov    $0x1,%esi
   aa791:	jmp    aa7a2 <clockcache_get_in_cache_async.constprop.0+0x322>
   aa793:	data16 cs nopw 0x0(%rax,%rax,1)
   aa79e:	xchg   %ax,%ax
   aa7a0:	pause
   aa7a2:	mov    %rsi,%rcx
   aa7a5:	xchg   %rcx,0x28(%rdx)
   aa7a9:	test   %rcx,%rcx
   aa7ac:	jne    aa7a0 <clockcache_get_in_cache_async.constprop.0+0x320>
   aa7ae:	mov    0x68(%r14),%rcx
   aa7b2:	mov    0x40(%r14),%rdx
   aa7b6:	lea    0x488(%r14),%r9
   aa7bd:	mov    0x38(%r14),%rsi
   aa7c1:	lea    0x28(%rcx),%rdi
   aa7c5:	movq   %rdx,%xmm0
   aa7ca:	movq   %rdi,%xmm1
   aa7cf:	punpcklqdq %xmm1,%xmm0
   aa7d3:	test   %rsi,%rsi
   aa7d6:	je     aab44 <clockcache_get_in_cache_async.constprop.0+0x6c4>
   aa7dc:	test   %rdx,%rdx
   aa7df:	je     aab1c <clockcache_get_in_cache_async.constprop.0+0x69c>
   aa7e5:	movabs $0x4153594e43575144,%rdx
   aa7ef:	cmp    %rdx,0x4a8(%r14)
   aa7f6:	je     aaa10 <clockcache_get_in_cache_async.constprop.0+0x590>
   aa7fc:	mov    %rdx,0x4a8(%r14)
   aa803:	movups %xmm0,0x498(%r14)
   aa80b:	mov    0x30(%rcx),%rdx
   aa80f:	mov    %rsi,0x490(%r14)
   aa816:	movq   $0x0,0x488(%r14)
   aa821:	test   %rdx,%rdx
   aa824:	je     aa96e <clockcache_get_in_cache_async.constprop.0+0x4ee>
   aa82a:	mov    0x38(%rcx),%rdx
   aa82e:	mov    %r9,(%rdx)
   aa831:	mov    %r9,0x38(%rcx)
   aa835:	lock orq $0x0,(%rsp)
   aa83b:	mov    0x20(%r14),%rsi
   aa83f:	mov    0x60(%r14),%ecx
   aa843:	shl    $0x6,%rcx
   aa847:	add    0x48(%rsi),%rcx
   aa84b:	mov    0x10(%rcx),%esi
   aa84e:	and    $0x10,%esi
   aa851:	mov    0x68(%r14),%rcx
   aa855:	je     aa86f <clockcache_get_in_cache_async.constprop.0+0x3ef>
   aa857:	lea    -0x3ba(%rip),%rsi        # aa4a4 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa85e:	mov    %rsi,0x18(%r14)
   aa862:	movq   $0x0,0x28(%rcx)
   aa86a:	jmp    aa508 <clockcache_get_in_cache_async.constprop.0+0x88>
   aa86f:	mov    0x4a8(%r14),%r8
   aa876:	mov    0x4a0(%r14),%rsi
   aa87d:	lea    0x28(%rcx),%rax
   aa881:	movabs $0x4153594e43575144,%rdi
   aa88b:	cmp    %rdi,%r8
   aa88e:	jne    aa9cf <clockcache_get_in_cache_async.constprop.0+0x54f>
   aa894:	cmp    %rsi,%rax
   aa897:	jne    aa9a5 <clockcache_get_in_cache_async.constprop.0+0x525>
   aa89d:	cmpq   $0x0,0x490(%r14)
   aa8a5:	je     aaa90 <clockcache_get_in_cache_async.constprop.0+0x610>
   aa8ab:	cmpq   $0x0,0x498(%r14)
   aa8b3:	je     aaa68 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aa8b9:	test   %rdx,%rdx
   aa8bc:	je     aa94a <clockcache_get_in_cache_async.constprop.0+0x4ca>
   aa8c2:	mov    0x20(%rdx),%r10
   aa8c6:	mov    0x18(%rdx),%rdi
   aa8ca:	cmp    %r8,%r10
   aa8cd:	jne    aaae5 <clockcache_get_in_cache_async.constprop.0+0x665>
   aa8d3:	cmp    %rax,%rdi
   aa8d6:	jne    aaab8 <clockcache_get_in_cache_async.constprop.0+0x638>
   aa8dc:	cmpq   $0x0,0x8(%rdx)
   aa8e1:	je     aaa90 <clockcache_get_in_cache_async.constprop.0+0x610>
   aa8e7:	cmpq   $0x0,0x10(%rdx)
   aa8ec:	je     aaa68 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aa8f2:	cmp    (%rdx),%r9
   aa8f5:	jne    aa97d <clockcache_get_in_cache_async.constprop.0+0x4fd>
   aa8fb:	mov    0x488(%r14),%rax
   aa902:	mov    %rax,(%rdx)
   aa905:	cmp    0x38(%rcx),%r9
   aa909:	je     aa977 <clockcache_get_in_cache_async.constprop.0+0x4f7>
   aa90b:	movabs $0x4153594e43574944,%rax
   aa915:	pxor   %xmm0,%xmm0
   aa919:	movq   $0x0,0x488(%r14)
   aa924:	movq   $0x0,0x490(%r14)
   aa92f:	mov    %rax,0x4a8(%r14)
   aa936:	movups %xmm0,0x498(%r14)
   aa93e:	movq   $0x0,(%rsi)
   aa945:	jmp    aa4a4 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa94a:	cmp    0x30(%rcx),%r9
   aa94e:	jne    aaa40 <clockcache_get_in_cache_async.constprop.0+0x5c0>
   aa954:	mov    0x488(%r14),%rax
   aa95b:	mov    %rax,0x30(%rcx)
   aa95f:	test   %rax,%rax
   aa962:	jne    aa90b <clockcache_get_in_cache_async.constprop.0+0x48b>
   aa964:	movq   $0x0,0x38(%rcx)
   aa96c:	jmp    aa90b <clockcache_get_in_cache_async.constprop.0+0x48b>
   aa96e:	mov    %r9,0x30(%rcx)
   aa972:	jmp    aa831 <clockcache_get_in_cache_async.constprop.0+0x3b1>
   aa977:	mov    %rdx,0x38(%rcx)
   aa97b:	jmp    aa90b <clockcache_get_in_cache_async.constprop.0+0x48b>
   aa97d:	lea    0x6bf6(%rip),%r8        # b157a <_fini+0x99e>
   aa984:	lea    0x887a(%rip),%rcx        # b3205 <_fini+0x2629>
   aa98b:	xor    %eax,%eax
   aa98d:	mov    $0x17a,%esi
   aa992:	lea    0x1ab57(%rip),%rdx        # c54f0 <__func__.12>
   aa999:	lea    0x6cff(%rip),%rdi        # b169f <_fini+0xac3>
   aa9a0:	call   ee00 <platform_assert_false@plt>
   aa9a5:	push   %rax
   aa9a6:	lea    0xdc13(%rip),%r8        # b85c0 <__func__.21+0x1100>
   aa9ad:	xor    %eax,%eax
   aa9af:	lea    0x6d04(%rip),%rcx        # b16ba <_fini+0xade>
   aa9b6:	push   %rsi
   aa9b7:	lea    0x17e32(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aa9be:	mov    $0x163,%esi
   aa9c3:	lea    0x6cd5(%rip),%rdi        # b169f <_fini+0xac3>
   aa9ca:	call   ee00 <platform_assert_false@plt>
   aa9cf:	push   0x498(%r14)
   aa9d6:	lea    0xdbb3(%rip),%rcx        # b8590 <__func__.21+0x10d0>
   aa9dd:	lea    0x17e0c(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aa9e4:	push   0x490(%r14)
   aa9eb:	lea    0x6cad(%rip),%rdi        # b169f <_fini+0xac3>
   aa9f2:	push   0x488(%r14)
   aa9f9:	push   %rax
   aa9fa:	xor    %eax,%eax
   aa9fc:	push   %rsi
   aa9fd:	mov    $0x159,%esi
   aaa02:	push   %r8
   aaa04:	lea    0xdb1d(%rip),%r8        # b8528 <__func__.21+0x1068>
   aaa0b:	call   ee00 <platform_assert_false@plt>
   aaa10:	push   %rdi
   aaa11:	lea    0x114a8(%rip),%r8        # bbec0 <__func__.21+0x4a00>
   aaa18:	mov    $0x13d,%esi
   aaa1d:	xor    %eax,%eax
   aaa1f:	push   0x4a0(%r14)
   aaa26:	lea    0x114cb(%rip),%rcx        # bbef8 <__func__.21+0x4a38>
   aaa2d:	lea    0x1aadc(%rip),%rdx        # c5510 <__func__.13>
   aaa34:	lea    0x6c64(%rip),%rdi        # b169f <_fini+0xac3>
   aaa3b:	call   ee00 <platform_assert_false@plt>
   aaa40:	lea    0x6b33(%rip),%r8        # b157a <_fini+0x99e>
   aaa47:	lea    0x87cc(%rip),%rcx        # b321a <_fini+0x263e>
   aaa4e:	xor    %eax,%eax
   aaa50:	mov    $0x180,%esi
   aaa55:	lea    0x1aa94(%rip),%rdx        # c54f0 <__func__.12>
   aaa5c:	lea    0x6c3c(%rip),%rdi        # b169f <_fini+0xac3>
   aaa63:	call   ee00 <platform_assert_false@plt>
   aaa68:	lea    0x6b0b(%rip),%r8        # b157a <_fini+0x99e>
   aaa6f:	lea    0x6c74(%rip),%rcx        # b16ea <_fini+0xb0e>
   aaa76:	xor    %eax,%eax
   aaa78:	mov    $0x169,%esi
   aaa7d:	lea    0x17d6c(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aaa84:	lea    0x6c14(%rip),%rdi        # b169f <_fini+0xac3>
   aaa8b:	call   ee00 <platform_assert_false@plt>
   aaa90:	lea    0x6ae3(%rip),%r8        # b157a <_fini+0x99e>
   aaa97:	lea    0x6c33(%rip),%rcx        # b16d1 <_fini+0xaf5>
   aaa9e:	xor    %eax,%eax
   aaaa0:	mov    $0x168,%esi
   aaaa5:	lea    0x17d44(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aaaac:	lea    0x6bec(%rip),%rdi        # b169f <_fini+0xac3>
   aaab3:	call   ee00 <platform_assert_false@plt>
   aaab8:	push   %rax
   aaab9:	mov    %rdx,%r9
   aaabc:	lea    0xdafd(%rip),%r8        # b85c0 <__func__.21+0x1100>
   aaac3:	mov    $0x163,%esi
   aaac8:	push   %rdi
   aaac9:	lea    0x6bea(%rip),%rcx        # b16ba <_fini+0xade>
   aaad0:	lea    0x17d19(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aaad7:	xor    %eax,%eax
   aaad9:	lea    0x6bbf(%rip),%rdi        # b169f <_fini+0xac3>
   aaae0:	call   ee00 <platform_assert_false@plt>
   aaae5:	push   0x10(%rdx)
   aaae8:	mov    %rdx,%r9
   aaaeb:	lea    0xda36(%rip),%r8        # b8528 <__func__.21+0x1068>
   aaaf2:	lea    0xda97(%rip),%rcx        # b8590 <__func__.21+0x10d0>
   aaaf9:	push   0x8(%rdx)
   aaafc:	mov    $0x159,%esi
   aab01:	push   (%rdx)
   aab03:	lea    0x17ce6(%rip),%rdx        # c27f0 <__func__.16.lto_priv.1.lto_priv.0>
   aab0a:	push   %rax
   aab0b:	xor    %eax,%eax
   aab0d:	push   %rdi
   aab0e:	lea    0x6b8a(%rip),%rdi        # b169f <_fini+0xac3>
   aab15:	push   %r10
   aab17:	call   ee00 <platform_assert_false@plt>
   aab1c:	lea    0x6a57(%rip),%r8        # b157a <_fini+0x99e>
   aab23:	lea    0x6bc8(%rip),%rcx        # b16f2 <_fini+0xb16>
   aab2a:	xor    %eax,%eax
   aab2c:	mov    $0x13c,%esi
   aab31:	lea    0x1a9d8(%rip),%rdx        # c5510 <__func__.13>
   aab38:	lea    0x6b60(%rip),%rdi        # b169f <_fini+0xac3>
   aab3f:	call   ee00 <platform_assert_false@plt>
   aab44:	lea    0x6a2f(%rip),%r8        # b157a <_fini+0x99e>
   aab4b:	lea    0x6b87(%rip),%rcx        # b16d9 <_fini+0xafd>
   aab52:	xor    %eax,%eax
   aab54:	mov    $0x13b,%esi
   aab59:	lea    0x1a9b0(%rip),%rdx        # c5510 <__func__.13>
   aab60:	lea    0x6b38(%rip),%rdi        # b169f <_fini+0xac3>
   aab67:	call   ee00 <platform_assert_false@plt>
   aab6c:	push   %r11
   aab6e:	lea    0x16903(%rip),%r8        # c1478 <__func__.21+0x9fb8>
   aab75:	mov    $0x9e2,%esi
   aab7a:	xor    %eax,%eax
   aab7c:	push   %rcx
   aab7d:	lea    0x1a9ac(%rip),%rdx        # c5530 <__func__.14>
   aab84:	lea    0x16915(%rip),%rcx        # c14a0 <__func__.21+0x9fe0>
   aab8b:	lea    0x6a45(%rip),%rdi        # b15d7 <_fini+0x9fb>
   aab92:	call   ee00 <platform_assert_false@plt>
   aab97:	push   %r10
   aab99:	lea    0x6a84(%rip),%rcx        # b1624 <_fini+0xa48>
   aaba0:	mov    $0x261,%esi
   aaba5:	xor    %eax,%eax
   aaba7:	push   %r9
   aaba9:	lea    0x17de0(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   aabb0:	xor    %r9d,%r9d
   aabb3:	push   %r8
   aabb5:	lea    0xd634(%rip),%r8        # b81f0 <__func__.21+0xd30>
   aabbc:	push   %rdi
   aabbd:	lea    0x6a13(%rip),%rdi        # b15d7 <_fini+0x9fb>
   aabc4:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
