
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009cc20 <clockcache_get_internal_async.constprop.0>:
   9cc20:	mov    0x10(%rdi),%rax
   9cc24:	cmp    $0x1,%rax
   9cc28:	je     9cd10 <clockcache_get_internal_async.constprop.0+0xf0>
   9cc2e:	push   %rbx
   9cc2f:	mov    %rdi,%rbx
   9cc32:	test   %rax,%rax
   9cc35:	je     9cc40 <clockcache_get_internal_async.constprop.0+0x20>
   9cc37:	jmp    *%rax
   9cc39:	nopl   0x0(%rax)
   9cc40:	data16 lea 0x38360(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   9cc48:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   9cc50:	mov    0x30(%rbx),%esi
   9cc53:	mov    0x28(%rbx),%r9
   9cc57:	mov    (%rax),%rax
   9cc5a:	mov    %rax,0x58(%rbx)
   9cc5e:	lea    -0x1(%rsi),%eax
   9cc61:	cmp    $0x7,%eax
   9cc64:	ja     9cd16 <clockcache_get_internal_async.constprop.0+0xf6>
   9cc6a:	mov    0x20(%rbx),%rsi
   9cc6e:	xor    %edx,%edx
   9cc70:	mov    0x8(%rsi),%rcx
   9cc74:	mov    0x8(%rcx),%rax
   9cc78:	mov    0x8(%rax),%rdi
   9cc7c:	mov    %r9,%rax
   9cc7f:	div    %rdi
   9cc82:	test   %rdx,%rdx
   9cc85:	jne    9cd1e <clockcache_get_internal_async.constprop.0+0xfe>
   9cc8b:	mov    0x120(%rcx),%rcx
   9cc92:	mov    0x28(%rsi),%rax
   9cc96:	mov    %r9,%rdx
   9cc99:	shr    %cl,%rdx
   9cc9c:	shr    $0x2,%rax
   9cca0:	cmp    %rax,%rdx
   9cca3:	jae    9cd48 <clockcache_get_internal_async.constprop.0+0x128>
   9cca9:	mov    0x30(%rsi),%rax
   9ccad:	mov    (%rax,%rdx,4),%eax
   9ccb0:	mov    $0xffffffff,%edx
   9ccb5:	movq   $0x0,0x18(%rbx)
   9ccbd:	mov    %rax,0x60(%rbx)
   9ccc1:	cmp    %rdx,%rax
   9ccc4:	je     9cce8 <clockcache_get_internal_async.constprop.0+0xc8>
   9ccc6:	lea    0x4(%rip),%rax        # 9ccd1 <clockcache_get_internal_async.constprop.0+0xb1>
   9cccd:	mov    %rax,0x10(%rbx)
   9ccd1:	mov    %rbx,%rdi
   9ccd4:	call   aa480 <clockcache_get_in_cache_async.constprop.0>
   9ccd9:	cmp    $0x1,%eax
   9ccdc:	je     9cd00 <clockcache_get_internal_async.constprop.0+0xe0>
   9ccde:	xor    %eax,%eax
   9cce0:	pop    %rbx
   9cce1:	ret
   9cce2:	nopw   0x0(%rax,%rax,1)
   9cce8:	lea    0x4(%rip),%rax        # 9ccf3 <clockcache_get_internal_async.constprop.0+0xd3>
   9ccef:	mov    %rax,0x10(%rbx)
   9ccf3:	mov    %rbx,%rdi
   9ccf6:	call   9c7d0 <clockcache_get_from_disk_async.constprop.0>
   9ccfb:	cmp    $0x1,%eax
   9ccfe:	jne    9ccde <clockcache_get_internal_async.constprop.0+0xbe>
   9cd00:	movq   $0x1,0x10(%rbx)
   9cd08:	mov    $0x1,%eax
   9cd0d:	pop    %rbx
   9cd0e:	ret
   9cd0f:	nop
   9cd10:	mov    $0x1,%eax
   9cd15:	ret
   9cd16:	mov    %r9,%rdi
   9cd19:	call   529a0 <clockcache_assert_page_type_valid.part.0.lto_priv.0>
   9cd1e:	push   %rcx
   9cd1f:	lea    0x1b52a(%rip),%r8        # b8250 <__func__.21+0xd90>
   9cd26:	mov    $0x18e,%esi
   9cd2b:	xor    %eax,%eax
   9cd2d:	push   %rdi
   9cd2e:	lea    0x148ff(%rip),%rcx        # b1634 <_fini+0xa58>
   9cd35:	lea    0x25c34(%rip),%rdx        # c2970 <__func__.33.lto_priv.0>
   9cd3c:	lea    0x14894(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9cd43:	call   ee00 <platform_assert_false@plt>
   9cd48:	imul   %rax,%rdi
   9cd4c:	push   %rdx
   9cd4d:	lea    0x1b534(%rip),%r8        # b8288 <__func__.21+0xdc8>
   9cd54:	xor    %eax,%eax
   9cd56:	lea    0x148ed(%rip),%rcx        # b164a <_fini+0xa6e>
   9cd5d:	lea    0x25c0c(%rip),%rdx        # c2970 <__func__.33.lto_priv.0>
   9cd64:	mov    $0x195,%esi
   9cd69:	push   %rdi
   9cd6a:	lea    0x14866(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9cd71:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
