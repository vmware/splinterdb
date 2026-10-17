
.cache/invalid-address-perf-20261017/builds/mask-main/release/lib/libsplinterdb.so:     file format elf64-x86-64


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
   9cc6a:	mov    0x20(%rbx),%rdx
   9cc6e:	mov    0x8(%rdx),%rax
   9cc72:	mov    0x8(%rax),%rcx
   9cc76:	mov    0x8(%rcx),%rsi
   9cc7a:	lea    -0x1(%rsi),%rcx
   9cc7e:	test   %r9,%rcx
   9cc81:	jne    9cd1e <clockcache_get_internal_async.constprop.0+0xfe>
   9cc87:	mov    0x120(%rax),%rcx
   9cc8e:	mov    %r9,%rax
   9cc91:	shr    %cl,%rax
   9cc94:	mov    %rax,%rcx
   9cc97:	mov    0x28(%rdx),%rax
   9cc9b:	shr    $0x2,%rax
   9cc9f:	cmp    %rax,%rcx
   9cca2:	jae    9cd48 <clockcache_get_internal_async.constprop.0+0x128>
   9cca8:	mov    0x30(%rdx),%rax
   9ccac:	mov    $0xffffffff,%edx
   9ccb1:	mov    (%rax,%rcx,4),%eax
   9ccb4:	movq   $0x0,0x18(%rbx)
   9ccbc:	mov    %rax,0x60(%rbx)
   9ccc0:	cmp    %rdx,%rax
   9ccc3:	je     9cce8 <clockcache_get_internal_async.constprop.0+0xc8>
   9ccc5:	lea    0x4(%rip),%rax        # 9ccd0 <clockcache_get_internal_async.constprop.0+0xb0>
   9cccc:	mov    %rax,0x10(%rbx)
   9ccd0:	mov    %rbx,%rdi
   9ccd3:	call   aa480 <clockcache_get_in_cache_async.constprop.0>
   9ccd8:	cmp    $0x1,%eax
   9ccdb:	je     9cd00 <clockcache_get_internal_async.constprop.0+0xe0>
   9ccdd:	xor    %eax,%eax
   9ccdf:	pop    %rbx
   9cce0:	ret
   9cce1:	nopl   0x0(%rax)
   9cce8:	lea    0x4(%rip),%rax        # 9ccf3 <clockcache_get_internal_async.constprop.0+0xd3>
   9ccef:	mov    %rax,0x10(%rbx)
   9ccf3:	mov    %rbx,%rdi
   9ccf6:	call   9c7d0 <clockcache_get_from_disk_async.constprop.0>
   9ccfb:	cmp    $0x1,%eax
   9ccfe:	jne    9ccdd <clockcache_get_internal_async.constprop.0+0xbd>
   9cd00:	movq   $0x1,0x10(%rbx)
   9cd08:	mov    $0x1,%eax
   9cd0d:	pop    %rbx
   9cd0e:	ret
   9cd0f:	nop
   9cd10:	mov    $0x1,%eax
   9cd15:	ret
   9cd16:	mov    %r9,%rdi
   9cd19:	call   52970 <clockcache_assert_page_type_valid.part.0.lto_priv.0>
   9cd1e:	push   %rcx
   9cd1f:	lea    0x1b53a(%rip),%r8        # b8260 <__func__.21+0xd90>
   9cd26:	lea    0x14907(%rip),%rcx        # b1634 <_fini+0xa18>
   9cd2d:	xor    %eax,%eax
   9cd2f:	push   %rsi
   9cd30:	lea    0x25c59(%rip),%rdx        # c2990 <__func__.33.lto_priv.0>
   9cd37:	mov    $0x18e,%esi
   9cd3c:	lea    0x14894(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9cd43:	call   ee00 <platform_assert_false@plt>
   9cd48:	imul   %rax,%rsi
   9cd4c:	push   %rdx
   9cd4d:	xor    %eax,%eax
   9cd4f:	lea    0x1b542(%rip),%r8        # b8298 <__func__.21+0xdc8>
   9cd56:	lea    0x148f5(%rip),%rcx        # b1652 <_fini+0xa36>
   9cd5d:	lea    0x25c2c(%rip),%rdx        # c2990 <__func__.33.lto_priv.0>
   9cd64:	lea    0x1486c(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9cd6b:	push   %rsi
   9cd6c:	mov    $0x195,%esi
   9cd71:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
