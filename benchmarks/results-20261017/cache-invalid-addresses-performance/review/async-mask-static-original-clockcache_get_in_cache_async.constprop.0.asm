
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa650 <clockcache_get_in_cache_async.constprop.0>:
   aa650:	mov    0x18(%rdi),%rax
   aa654:	cmp    $0x1,%rax
   aa658:	je     aa8c0 <clockcache_get_in_cache_async.constprop.0+0x270>
   aa65e:	push   %r15
   aa660:	push   %r14
   aa662:	mov    %rdi,%r14
   aa665:	push   %r13
   aa667:	push   %r12
   aa669:	push   %rbp
   aa66a:	push   %rbx
   aa66b:	sub    $0x8,%rsp
   aa66f:	test   %rax,%rax
   aa672:	je     aa6f0 <clockcache_get_in_cache_async.constprop.0+0xa0>
   aa674:	mov    0x20(%r14),%rsi
   aa678:	mov    0x60(%r14),%eax
   aa67c:	xor    %edx,%edx
   aa67e:	mov    0x58(%r14),%rdi
   aa682:	mov    0x8(%rsi),%rcx
   aa686:	mov    %rdi,%r9
   aa689:	divq   0x140(%rcx)
   aa690:	and    $0x3,%r9d
   aa694:	shl    $0x6,%edx
   aa697:	lea    (%rdx,%rax,1),%r8d
   aa69b:	mov    0x130(%rcx),%eax
   aa6a1:	mov    $0xffffffff,%ecx
   aa6a6:	mov    0x88(%rsi),%rdx
   aa6ad:	imul   %r9,%rax
   aa6b1:	add    %r8,%rax
   aa6b4:	lock xadd %cx,(%rdx,%rax,2)
   aa6ba:	test   %cx,%cx
   aa6bd:	je     aad67 <clockcache_get_in_cache_async.constprop.0+0x717>
   aa6c3:	movl   $0x0,0x54(%r14)
   aa6cb:	movq   $0x1,0x18(%r14)
   aa6d3:	mov    $0x1,%eax
   aa6d8:	add    $0x8,%rsp
   aa6dc:	pop    %rbx
   aa6dd:	pop    %rbp
   aa6de:	pop    %r12
   aa6e0:	pop    %r13
   aa6e2:	pop    %r14
   aa6e4:	pop    %r15
   aa6e6:	ret
   aa6e7:	nopw   0x0(%rax,%rax,1)
   aa6f0:	data16 lea 0x2b8b0(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   aa6f8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa700:	mov    0x60(%r14),%r12d
   aa704:	mov    0x20(%r14),%rbp
   aa708:	mov    %r12d,%esi
   aa70b:	mov    %rbp,%rdi
   aa70e:	mov    (%rax),%rax
   aa711:	mov    %rax,0x58(%r14)
   aa715:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   aa71a:	cmp    $0x1,%eax
   aa71d:	jne    aa76e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa71f:	movabs $0xcccccccccccccccd,%r13
   aa729:	mov    $0x1,%r15d
   aa72f:	mov    %r15,%rax
   aa732:	mul    %r13
   aa735:	xor    %eax,%eax
   aa737:	mov    %rdx,%rbx
   aa73a:	shr    $0x2,%rbx
   aa73e:	xchg   %ax,%ax
   aa740:	pause
   aa742:	mov    %rax,%rcx
   aa745:	add    $0x1,%rax
   aa749:	cmp    %rcx,%rbx
   aa74c:	jne    aa740 <clockcache_get_in_cache_async.constprop.0+0xf0>
   aa74e:	cmp    $0x400,%r15
   aa755:	ja     aa818 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa75b:	mov    %r12d,%esi
   aa75e:	mov    %rbp,%rdi
   aa761:	add    %r15,%r15
   aa764:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   aa769:	cmp    $0x1,%eax
   aa76c:	je     aa72f <clockcache_get_in_cache_async.constprop.0+0xdf>
   aa76e:	test   %eax,%eax
   aa770:	jne    aa6c3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa776:	mov    0x20(%r14),%rsi
   aa77a:	mov    0x60(%r14),%edi
   aa77e:	mov    0x48(%rsi),%rcx
   aa782:	mov    %rdi,%rdx
   aa785:	shl    $0x6,%rdx
   aa789:	add    %rdx,%rcx
   aa78c:	mov    %rcx,0x68(%r14)
   aa790:	mov    0x8(%rcx),%r8
   aa794:	cmp    0x28(%r14),%r8
   aa798:	jne    aa8c6 <clockcache_get_in_cache_async.constprop.0+0x276>
   aa79e:	mov    0x20(%rcx),%r9d
   aa7a2:	mov    0x30(%r14),%ecx
   aa7a6:	cmp    %ecx,%r9d
   aa7a9:	jne    aad3c <clockcache_get_in_cache_async.constprop.0+0x6ec>
   aa7af:	add    0x48(%rsi),%rdx
   aa7b3:	mov    0x10(%rdx),%edx
   aa7b6:	and    $0x10,%edx
   aa7b9:	mov    0x20(%r14),%rcx
   aa7bd:	jne    aa940 <clockcache_get_in_cache_async.constprop.0+0x2f0>
   aa7c3:	mov    0x68(%r14),%rax
   aa7c7:	mov    0x8(%rcx),%rsi
   aa7cb:	mov    0x14(%rax),%edi
   aa7ce:	test   %edi,%edi
   aa7d0:	jne    aa840 <clockcache_get_in_cache_async.constprop.0+0x1f0>
   aa7d2:	mov    0x18(%rsi),%edx
   aa7d5:	test   %edx,%edx
   aa7d7:	je     aa7f9 <clockcache_get_in_cache_async.constprop.0+0x1a9>
   aa7d9:	mov    0x58(%r14),%rsi
   aa7dd:	mov    0x30(%r14),%edi
   aa7e1:	lea    0x0(,%rsi,8),%rdx
   aa7e9:	sub    %rsi,%rdx
   aa7ec:	lea    0x220(%rdi,%rdx,8),%rdx
   aa7f4:	addq   $0x1,(%rcx,%rdx,8)
   aa7f9:	mov    %rax,0x48(%r14)
   aa7fd:	movl   $0x1,0x54(%r14)
   aa805:	movq   $0x1,0x18(%r14)
   aa80d:	jmp    aa6d3 <clockcache_get_in_cache_async.constprop.0+0x83>
   aa812:	nopw   0x0(%rax,%rax,1)
   aa818:	mov    %r12d,%esi
   aa81b:	mov    %rbp,%rdi
   aa81e:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   aa823:	cmp    $0x1,%eax
   aa826:	jne    aa76e <clockcache_get_in_cache_async.constprop.0+0x11e>
   aa82c:	xor    %eax,%eax
   aa82e:	xchg   %ax,%ax
   aa830:	pause
   aa832:	mov    %rax,%rdx
   aa835:	add    $0x1,%rax
   aa839:	cmp    %rdx,%rbx
   aa83c:	jne    aa830 <clockcache_get_in_cache_async.constprop.0+0x1e0>
   aa83e:	jmp    aa818 <clockcache_get_in_cache_async.constprop.0+0x1c8>
   aa840:	mov    0x14(%rax),%eax
   aa843:	xor    %edx,%edx
   aa845:	mov    0x58(%r14),%rdi
   aa849:	mov    %eax,0x50(%r14)
   aa84d:	mov    0x60(%r14),%eax
   aa851:	mov    %rdi,%r9
   aa854:	and    $0x3,%r9d
   aa858:	divq   0x140(%rsi)
   aa85f:	shl    $0x6,%edx
   aa862:	lea    (%rdx,%rax,1),%r8d
   aa866:	mov    0x130(%rsi),%eax
   aa86c:	mov    0x88(%rcx),%rdx
   aa873:	mov    $0xffffffff,%ecx
   aa878:	imul   %r9,%rax
   aa87c:	add    %r8,%rax
   aa87f:	lock xadd %cx,(%rdx,%rax,2)
   aa885:	test   %cx,%cx
   aa888:	jne    aa7fd <clockcache_get_in_cache_async.constprop.0+0x1ad>
   aa88e:	push   %rcx
   aa88f:	lea    0x1915a(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa896:	mov    $0x27c,%esi
   aa89b:	xor    %eax,%eax
   aa89d:	push   %r9
   aa89f:	lea    0x7d88(%rip),%rcx        # b262e <_fini+0x1512>
   aa8a6:	xor    %r9d,%r9d
   aa8a9:	push   %r8
   aa8ab:	lea    0xe8d6(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa8b2:	push   %rdi
   aa8b3:	lea    0x7d1d(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa8ba:	call   ee00 <platform_assert_false@plt>
   aa8bf:	nop
   aa8c0:	mov    $0x1,%eax
   aa8c5:	ret
   aa8c6:	mov    0x8(%rsi),%rcx
   aa8ca:	mov    %rdi,%rax
   aa8cd:	xor    %edx,%edx
   aa8cf:	mov    0x58(%r14),%r8
   aa8d3:	divq   0x140(%rcx)
   aa8da:	mov    %r8,%r9
   aa8dd:	and    $0x3,%r9d
   aa8e1:	shl    $0x6,%edx
   aa8e4:	lea    (%rdx,%rax,1),%edi
   aa8e7:	mov    0x130(%rcx),%eax
   aa8ed:	mov    0x88(%rsi),%rdx
   aa8f4:	mov    $0xffffffff,%ecx
   aa8f9:	imul   %r9,%rax
   aa8fd:	add    %rdi,%rax
   aa900:	lock xadd %cx,(%rdx,%rax,2)
   aa906:	test   %cx,%cx
   aa909:	jne    aa6c3 <clockcache_get_in_cache_async.constprop.0+0x73>
   aa90f:	push   %rbx
   aa910:	lea    0x7d17(%rip),%rcx        # b262e <_fini+0x1512>
   aa917:	mov    $0x27c,%esi
   aa91c:	xor    %eax,%eax
   aa91e:	push   %r9
   aa920:	lea    0x190c9(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa927:	xor    %r9d,%r9d
   aa92a:	push   %rdi
   aa92b:	lea    0x7ca5(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa932:	push   %r8
   aa934:	lea    0xe84d(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa93b:	call   ee00 <platform_assert_false@plt>
   aa940:	mov    0x60(%r14),%edx
   aa944:	shl    $0x6,%rdx
   aa948:	add    0x48(%rcx),%rdx
   aa94c:	mov    0x10(%rdx),%edx
   aa94f:	and    $0x10,%edx
   aa952:	je     aa674 <clockcache_get_in_cache_async.constprop.0+0x24>
   aa958:	mov    0x68(%r14),%rdx
   aa95c:	mov    $0x1,%esi
   aa961:	jmp    aa972 <clockcache_get_in_cache_async.constprop.0+0x322>
   aa963:	data16 cs nopw 0x0(%rax,%rax,1)
   aa96e:	xchg   %ax,%ax
   aa970:	pause
   aa972:	mov    %rsi,%rcx
   aa975:	xchg   %rcx,0x28(%rdx)
   aa979:	test   %rcx,%rcx
   aa97c:	jne    aa970 <clockcache_get_in_cache_async.constprop.0+0x320>
   aa97e:	mov    0x68(%r14),%rcx
   aa982:	mov    0x40(%r14),%rdx
   aa986:	lea    0x488(%r14),%r9
   aa98d:	mov    0x38(%r14),%rsi
   aa991:	lea    0x28(%rcx),%rdi
   aa995:	movq   %rdx,%xmm0
   aa99a:	movq   %rdi,%xmm1
   aa99f:	punpcklqdq %xmm1,%xmm0
   aa9a3:	test   %rsi,%rsi
   aa9a6:	je     aad14 <clockcache_get_in_cache_async.constprop.0+0x6c4>
   aa9ac:	test   %rdx,%rdx
   aa9af:	je     aacec <clockcache_get_in_cache_async.constprop.0+0x69c>
   aa9b5:	movabs $0x4153594e43575144,%rdx
   aa9bf:	cmp    %rdx,0x4a8(%r14)
   aa9c6:	je     aabe0 <clockcache_get_in_cache_async.constprop.0+0x590>
   aa9cc:	mov    %rdx,0x4a8(%r14)
   aa9d3:	movups %xmm0,0x498(%r14)
   aa9db:	mov    0x30(%rcx),%rdx
   aa9df:	mov    %rsi,0x490(%r14)
   aa9e6:	movq   $0x0,0x488(%r14)
   aa9f1:	test   %rdx,%rdx
   aa9f4:	je     aab3e <clockcache_get_in_cache_async.constprop.0+0x4ee>
   aa9fa:	mov    0x38(%rcx),%rdx
   aa9fe:	mov    %r9,(%rdx)
   aaa01:	mov    %r9,0x38(%rcx)
   aaa05:	lock orq $0x0,(%rsp)
   aaa0b:	mov    0x20(%r14),%rsi
   aaa0f:	mov    0x60(%r14),%ecx
   aaa13:	shl    $0x6,%rcx
   aaa17:	add    0x48(%rsi),%rcx
   aaa1b:	mov    0x10(%rcx),%esi
   aaa1e:	and    $0x10,%esi
   aaa21:	mov    0x68(%r14),%rcx
   aaa25:	je     aaa3f <clockcache_get_in_cache_async.constprop.0+0x3ef>
   aaa27:	lea    -0x3ba(%rip),%rsi        # aa674 <clockcache_get_in_cache_async.constprop.0+0x24>
   aaa2e:	mov    %rsi,0x18(%r14)
   aaa32:	movq   $0x0,0x28(%rcx)
   aaa3a:	jmp    aa6d8 <clockcache_get_in_cache_async.constprop.0+0x88>
   aaa3f:	mov    0x4a8(%r14),%r8
   aaa46:	mov    0x4a0(%r14),%rsi
   aaa4d:	lea    0x28(%rcx),%rax
   aaa51:	movabs $0x4153594e43575144,%rdi
   aaa5b:	cmp    %rdi,%r8
   aaa5e:	jne    aab9f <clockcache_get_in_cache_async.constprop.0+0x54f>
   aaa64:	cmp    %rsi,%rax
   aaa67:	jne    aab75 <clockcache_get_in_cache_async.constprop.0+0x525>
   aaa6d:	cmpq   $0x0,0x490(%r14)
   aaa75:	je     aac60 <clockcache_get_in_cache_async.constprop.0+0x610>
   aaa7b:	cmpq   $0x0,0x498(%r14)
   aaa83:	je     aac38 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aaa89:	test   %rdx,%rdx
   aaa8c:	je     aab1a <clockcache_get_in_cache_async.constprop.0+0x4ca>
   aaa92:	mov    0x20(%rdx),%r10
   aaa96:	mov    0x18(%rdx),%rdi
   aaa9a:	cmp    %r8,%r10
   aaa9d:	jne    aacb5 <clockcache_get_in_cache_async.constprop.0+0x665>
   aaaa3:	cmp    %rax,%rdi
   aaaa6:	jne    aac88 <clockcache_get_in_cache_async.constprop.0+0x638>
   aaaac:	cmpq   $0x0,0x8(%rdx)
   aaab1:	je     aac60 <clockcache_get_in_cache_async.constprop.0+0x610>
   aaab7:	cmpq   $0x0,0x10(%rdx)
   aaabc:	je     aac38 <clockcache_get_in_cache_async.constprop.0+0x5e8>
   aaac2:	cmp    (%rdx),%r9
   aaac5:	jne    aab4d <clockcache_get_in_cache_async.constprop.0+0x4fd>
   aaacb:	mov    0x488(%r14),%rax
   aaad2:	mov    %rax,(%rdx)
   aaad5:	cmp    0x38(%rcx),%r9
   aaad9:	je     aab47 <clockcache_get_in_cache_async.constprop.0+0x4f7>
   aaadb:	movabs $0x4153594e43574944,%rax
   aaae5:	pxor   %xmm0,%xmm0
   aaae9:	movq   $0x0,0x488(%r14)
   aaaf4:	movq   $0x0,0x490(%r14)
   aaaff:	mov    %rax,0x4a8(%r14)
   aab06:	movups %xmm0,0x498(%r14)
   aab0e:	movq   $0x0,(%rsi)
   aab15:	jmp    aa674 <clockcache_get_in_cache_async.constprop.0+0x24>
   aab1a:	cmp    0x30(%rcx),%r9
   aab1e:	jne    aac10 <clockcache_get_in_cache_async.constprop.0+0x5c0>
   aab24:	mov    0x488(%r14),%rax
   aab2b:	mov    %rax,0x30(%rcx)
   aab2f:	test   %rax,%rax
   aab32:	jne    aaadb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aab34:	movq   $0x0,0x38(%rcx)
   aab3c:	jmp    aaadb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aab3e:	mov    %r9,0x30(%rcx)
   aab42:	jmp    aaa01 <clockcache_get_in_cache_async.constprop.0+0x3b1>
   aab47:	mov    %rdx,0x38(%rcx)
   aab4b:	jmp    aaadb <clockcache_get_in_cache_async.constprop.0+0x48b>
   aab4d:	lea    0x7a26(%rip),%r8        # b257a <_fini+0x145e>
   aab54:	lea    0x9678(%rip),%rcx        # b41d3 <_fini+0x30b7>
   aab5b:	xor    %eax,%eax
   aab5d:	mov    $0x17a,%esi
   aab62:	lea    0x1ba07(%rip),%rdx        # c6570 <__func__.12>
   aab69:	lea    0x7b32(%rip),%rdi        # b26a2 <_fini+0x1586>
   aab70:	call   ee00 <platform_assert_false@plt>
   aab75:	push   %rax
   aab76:	lea    0xeceb(%rip),%r8        # b9868 <__func__.21+0x13d8>
   aab7d:	xor    %eax,%eax
   aab7f:	lea    0x7b37(%rip),%rcx        # b26bd <_fini+0x15a1>
   aab86:	push   %rsi
   aab87:	lea    0x18c42(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aab8e:	mov    $0x163,%esi
   aab93:	lea    0x7b08(%rip),%rdi        # b26a2 <_fini+0x1586>
   aab9a:	call   ee00 <platform_assert_false@plt>
   aab9f:	push   0x498(%r14)
   aaba6:	lea    0xec8b(%rip),%rcx        # b9838 <__func__.21+0x13a8>
   aabad:	lea    0x18c1c(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aabb4:	push   0x490(%r14)
   aabbb:	lea    0x7ae0(%rip),%rdi        # b26a2 <_fini+0x1586>
   aabc2:	push   0x488(%r14)
   aabc9:	push   %rax
   aabca:	xor    %eax,%eax
   aabcc:	push   %rsi
   aabcd:	mov    $0x159,%esi
   aabd2:	push   %r8
   aabd4:	lea    0xebf5(%rip),%r8        # b97d0 <__func__.21+0x1340>
   aabdb:	call   ee00 <platform_assert_false@plt>
   aabe0:	push   %rdi
   aabe1:	lea    0x12310(%rip),%r8        # bcef8 <__func__.21+0x4a68>
   aabe8:	mov    $0x13d,%esi
   aabed:	xor    %eax,%eax
   aabef:	push   0x4a0(%r14)
   aabf6:	lea    0x12333(%rip),%rcx        # bcf30 <__func__.21+0x4aa0>
   aabfd:	lea    0x1b98c(%rip),%rdx        # c6590 <__func__.13>
   aac04:	lea    0x7a97(%rip),%rdi        # b26a2 <_fini+0x1586>
   aac0b:	call   ee00 <platform_assert_false@plt>
   aac10:	lea    0x7963(%rip),%r8        # b257a <_fini+0x145e>
   aac17:	lea    0x95ca(%rip),%rcx        # b41e8 <_fini+0x30cc>
   aac1e:	xor    %eax,%eax
   aac20:	mov    $0x180,%esi
   aac25:	lea    0x1b944(%rip),%rdx        # c6570 <__func__.12>
   aac2c:	lea    0x7a6f(%rip),%rdi        # b26a2 <_fini+0x1586>
   aac33:	call   ee00 <platform_assert_false@plt>
   aac38:	lea    0x793b(%rip),%r8        # b257a <_fini+0x145e>
   aac3f:	lea    0x7aa7(%rip),%rcx        # b26ed <_fini+0x15d1>
   aac46:	xor    %eax,%eax
   aac48:	mov    $0x169,%esi
   aac4d:	lea    0x18b7c(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aac54:	lea    0x7a47(%rip),%rdi        # b26a2 <_fini+0x1586>
   aac5b:	call   ee00 <platform_assert_false@plt>
   aac60:	lea    0x7913(%rip),%r8        # b257a <_fini+0x145e>
   aac67:	lea    0x7a66(%rip),%rcx        # b26d4 <_fini+0x15b8>
   aac6e:	xor    %eax,%eax
   aac70:	mov    $0x168,%esi
   aac75:	lea    0x18b54(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aac7c:	lea    0x7a1f(%rip),%rdi        # b26a2 <_fini+0x1586>
   aac83:	call   ee00 <platform_assert_false@plt>
   aac88:	push   %rax
   aac89:	mov    %rdx,%r9
   aac8c:	lea    0xebd5(%rip),%r8        # b9868 <__func__.21+0x13d8>
   aac93:	mov    $0x163,%esi
   aac98:	push   %rdi
   aac99:	lea    0x7a1d(%rip),%rcx        # b26bd <_fini+0x15a1>
   aaca0:	lea    0x18b29(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aaca7:	xor    %eax,%eax
   aaca9:	lea    0x79f2(%rip),%rdi        # b26a2 <_fini+0x1586>
   aacb0:	call   ee00 <platform_assert_false@plt>
   aacb5:	push   0x10(%rdx)
   aacb8:	mov    %rdx,%r9
   aacbb:	lea    0xeb0e(%rip),%r8        # b97d0 <__func__.21+0x1340>
   aacc2:	lea    0xeb6f(%rip),%rcx        # b9838 <__func__.21+0x13a8>
   aacc9:	push   0x8(%rdx)
   aaccc:	mov    $0x159,%esi
   aacd1:	push   (%rdx)
   aacd3:	lea    0x18af6(%rip),%rdx        # c37d0 <__func__.16.lto_priv.1.lto_priv.0>
   aacda:	push   %rax
   aacdb:	xor    %eax,%eax
   aacdd:	push   %rdi
   aacde:	lea    0x79bd(%rip),%rdi        # b26a2 <_fini+0x1586>
   aace5:	push   %r10
   aace7:	call   ee00 <platform_assert_false@plt>
   aacec:	lea    0x7887(%rip),%r8        # b257a <_fini+0x145e>
   aacf3:	lea    0x79fb(%rip),%rcx        # b26f5 <_fini+0x15d9>
   aacfa:	xor    %eax,%eax
   aacfc:	mov    $0x13c,%esi
   aad01:	lea    0x1b888(%rip),%rdx        # c6590 <__func__.13>
   aad08:	lea    0x7993(%rip),%rdi        # b26a2 <_fini+0x1586>
   aad0f:	call   ee00 <platform_assert_false@plt>
   aad14:	lea    0x785f(%rip),%r8        # b257a <_fini+0x145e>
   aad1b:	lea    0x79ba(%rip),%rcx        # b26dc <_fini+0x15c0>
   aad22:	xor    %eax,%eax
   aad24:	mov    $0x13b,%esi
   aad29:	lea    0x1b860(%rip),%rdx        # c6590 <__func__.13>
   aad30:	lea    0x796b(%rip),%rdi        # b26a2 <_fini+0x1586>
   aad37:	call   ee00 <platform_assert_false@plt>
   aad3c:	push   %r11
   aad3e:	lea    0x1776b(%rip),%r8        # c24b0 <__func__.21+0xa020>
   aad45:	mov    $0xa02,%esi
   aad4a:	xor    %eax,%eax
   aad4c:	push   %rcx
   aad4d:	lea    0x1b85c(%rip),%rdx        # c65b0 <__func__.14>
   aad54:	lea    0x1777d(%rip),%rcx        # c24d8 <__func__.21+0xa048>
   aad5b:	lea    0x7875(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aad62:	call   ee00 <platform_assert_false@plt>
   aad67:	push   %r10
   aad69:	lea    0x78be(%rip),%rcx        # b262e <_fini+0x1512>
   aad70:	mov    $0x27c,%esi
   aad75:	xor    %eax,%eax
   aad77:	push   %r9
   aad79:	lea    0x18c70(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aad80:	xor    %r9d,%r9d
   aad83:	push   %r8
   aad85:	lea    0xe3fc(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aad8c:	push   %rdi
   aad8d:	lea    0x7843(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aad94:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
