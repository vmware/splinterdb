
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000049a50 <btree_iterator_next>:
   49a50:	push   %rbp
   49a51:	push   %rbx
   49a52:	sub    $0x18,%rsp
   49a56:	mov    0xb4(%rdi),%ebp
   49a5c:	test   %ebp,%ebp
   49a5e:	je     49a70 <btree_iterator_next+0x20>
   49a60:	add    $0x18,%rsp
   49a64:	mov    %ebp,%eax
   49a66:	pop    %rbx
   49a67:	pop    %rbp
   49a68:	ret
   49a69:	nopl   0x0(%rax)
   49a70:	mov    0xd8(%rdi),%esi
   49a76:	mov    %rdi,%rbx
   49a79:	test   %esi,%esi
   49a7b:	je     49a98 <btree_iterator_next+0x48>
   49a7d:	mov    0xcc(%rdi),%ecx
   49a83:	test   %ecx,%ecx
   49a85:	jne    49c30 <btree_iterator_next+0x1e0>
   49a8b:	mov    $0x1,%esi
   49a90:	mov    %rbx,%rdi
   49a93:	call   55350 <btree_iterator_maybe_start_prefetch.part.0.lto_priv.0>
   49a98:	mov    0x80(%rbx),%rdx
   49a9f:	mov    0x90(%rbx),%rax
   49aa6:	movzwl 0x24(%rdx),%edx
   49aaa:	add    $0x1,%rax
   49aae:	mov    %rax,0x90(%rbx)
   49ab5:	cmp    %rdx,%rax
   49ab8:	jne    49af0 <btree_iterator_next+0xa0>
   49aba:	mov    0xb4(%rbx),%edx
   49ac0:	test   %edx,%edx
   49ac2:	jne    49af0 <btree_iterator_next+0xa0>
   49ac4:	mov    0xa0(%rbx),%rcx
   49acb:	cmp    %rcx,0x68(%rbx)
   49acf:	je     49cf0 <btree_iterator_next+0x2a0>
   49ad5:	mov    %rbx,%rdi
   49ad8:	call   493e0 <btree_iterator_next_leaf>
   49add:	test   %eax,%eax
   49adf:	jne    49d50 <btree_iterator_next+0x300>
   49ae5:	data16 cs nopw 0x0(%rax,%rax,1)
   49af0:	movq   $0x0,0xb8(%rbx)
   49afb:	cmpl   $0x2,0x1c(%rbx)
   49aff:	jne    49a60 <btree_iterator_next+0x10>
   49b05:	mov    0xb4(%rbx),%eax
   49b0b:	test   %eax,%eax
   49b0d:	jne    49a60 <btree_iterator_next+0x10>
   49b13:	mov    0x90(%rbx),%rdx
   49b1a:	mov    0xa0(%rbx),%rax
   49b21:	cmp    %rax,0x68(%rbx)
   49b25:	je     49d18 <btree_iterator_next+0x2c8>
   49b2b:	cmp    %rdx,0x98(%rbx)
   49b32:	jg     49ce8 <btree_iterator_next+0x298>
   49b38:	mov    0x80(%rbx),%rax
   49b3f:	mov    0x10(%rbx),%rsi
   49b43:	cmpb   $0x0,0x20(%rax)
   49b47:	movzwl 0x24(%rax),%ecx
   49b4b:	jne    49c70 <btree_iterator_next+0x220>
   49b51:	cmp    %cx,%dx
   49b54:	jae    49d60 <btree_iterator_next+0x310>
   49b5a:	movzwl %dx,%edx
   49b5d:	movzwl 0x28(%rax,%rdx,2),%ecx
   49b62:	cmp    0x22(%rax),%cx
   49b66:	jb     49d60 <btree_iterator_next+0x310>
   49b6c:	mov    0x10(%rsi),%rdx
   49b70:	movzwl 0x26(%rax),%r8d
   49b75:	lea    0x6(%rcx),%rsi
   49b79:	mov    %rdx,%r9
   49b7c:	mov    %r8,%rdi
   49b7f:	sub    %r8,%r9
   49b82:	cmp    %rsi,%r9
   49b85:	jb     49d60 <btree_iterator_next+0x310>
   49b8b:	lea    (%rax,%rcx,1),%rsi
   49b8f:	cmpb   $0x0,0x2(%rsi)
   49b93:	jne    49d60 <btree_iterator_next+0x310>
   49b99:	movzwl (%rsi),%eax
   49b9c:	mov    %rdx,%r10
   49b9f:	mov    $0x16,%ebp
   49ba4:	shr    $0x3,%r10
   49ba8:	lea    (%rax,%r8,1),%r9
   49bac:	cmp    %r9,%r10
   49baf:	jb     49c10 <btree_iterator_next+0x1c0>
   49bb1:	movzbl 0x5(%rsi),%r9d
   49bb6:	test   $0x3,%r9b
   49bba:	je     49d60 <btree_iterator_next+0x310>
   49bc0:	cmp    $0x7,%r9b
   49bc4:	ja     49d60 <btree_iterator_next+0x310>
   49bca:	movzwl 0x3(%rsi),%r10d
   49bcf:	add    %rax,%r10
   49bd2:	lea    -0x6(%rdx),%rax
   49bd6:	sub    %r8,%rax
   49bd9:	sub    %rcx,%rax
   49bdc:	cmp    %r10,%rax
   49bdf:	jb     49c10 <btree_iterator_next+0x1c0>
   49be1:	and    $0x4,%r9d
   49be5:	jne    49d6a <btree_iterator_next+0x31a>
   49beb:	mov    %rsi,0xb8(%rbx)
   49bf2:	test   %di,%di
   49bf5:	je     49ce8 <btree_iterator_next+0x298>
   49bfb:	mov    %rbx,%rdi
   49bfe:	call   44bd0 <btree_iterator_decode_current>
   49c03:	mov    %eax,%ebp
   49c05:	test   %eax,%eax
   49c07:	je     49ce8 <btree_iterator_next+0x298>
   49c0d:	nopl   (%rax)
   49c10:	movq   $0x0,0xb8(%rbx)
   49c1b:	mov    %ebp,0xb4(%rbx)
   49c21:	jmp    49a60 <btree_iterator_next+0x10>
   49c26:	cs nopw 0x0(%rax,%rax,1)
   49c30:	cmpl   $0x1,0xd4(%rdi)
   49c37:	je     49a98 <btree_iterator_next+0x48>
   49c3d:	lea    0xf8(%rdi),%rdi
   49c44:	call   ea30 <mini_meta_cursor_deinit@plt>
   49c49:	movl   $0x0,0xc8(%rbx)
   49c53:	movq   $0x0,0xcc(%rbx)
   49c5e:	movq   $0x0,0xf0(%rbx)
   49c69:	jmp    49a8b <btree_iterator_next+0x3b>
   49c6e:	xchg   %ax,%ax
   49c70:	cmp    %cx,%dx
   49c73:	jae    49d60 <btree_iterator_next+0x310>
   49c79:	movzwl %dx,%edx
   49c7c:	movzwl 0x28(%rax,%rdx,2),%edx
   49c81:	cmp    0x22(%rax),%dx
   49c85:	jb     49d60 <btree_iterator_next+0x310>
   49c8b:	movzwl 0x26(%rax),%ecx
   49c8f:	mov    0x10(%rsi),%rsi
   49c93:	sub    %rcx,%rsi
   49c96:	lea    0x17(%rdx),%rcx
   49c9a:	cmp    %rcx,%rsi
   49c9d:	jb     49d60 <btree_iterator_next+0x310>
   49ca3:	add    %rdx,%rax
   49ca6:	movzbl 0x16(%rax),%edi
   49caa:	cmp    $0x1,%dil
   49cae:	ja     49d60 <btree_iterator_next+0x310>
   49cb4:	movzwl 0x14(%rax),%ecx
   49cb8:	mov    %rcx,%r8
   49cbb:	add    $0x17,%rcx
   49cbf:	cmp    $0xfffd,%r8w
   49cc4:	mov    $0x17,%r8d
   49cca:	cmova  %r8,%rcx
   49cce:	test   %dil,%dil
   49cd1:	lea    0xc(%rcx),%r8
   49cd5:	cmovne %r8,%rcx
   49cd9:	sub    %rdx,%rsi
   49cdc:	cmp    %rcx,%rsi
   49cdf:	jb     49d60 <btree_iterator_next+0x310>
   49ce1:	mov    %rax,0xb8(%rbx)
   49ce8:	xor    %ebp,%ebp
   49cea:	jmp    49a60 <btree_iterator_next+0x10>
   49cef:	nop
   49cf0:	mov    0xa8(%rbx),%rdx
   49cf7:	cmp    %rdx,%rax
   49cfa:	jge    49af0 <btree_iterator_next+0xa0>
   49d00:	cmp    0x98(%rbx),%rdx
   49d07:	jne    49ad5 <btree_iterator_next+0x85>
   49d0d:	jmp    49af0 <btree_iterator_next+0xa0>
   49d12:	nopw   0x0(%rax,%rax,1)
   49d18:	mov    0xa8(%rbx),%rax
   49d1f:	xor    %ebp,%ebp
   49d21:	cmp    %rdx,%rax
   49d24:	jle    49a60 <btree_iterator_next+0x10>
   49d2a:	mov    0x98(%rbx),%rcx
   49d31:	cmp    %rcx,%rax
   49d34:	je     49a60 <btree_iterator_next+0x10>
   49d3a:	cmp    %rdx,%rcx
   49d3d:	jg     49a60 <btree_iterator_next+0x10>
   49d43:	jmp    49b38 <btree_iterator_next+0xe8>
   49d48:	nopl   0x0(%rax,%rax,1)
   49d50:	mov    %eax,%ebp
   49d52:	jmp    49a60 <btree_iterator_next+0x10>
   49d57:	nopw   0x0(%rax,%rax,1)
   49d60:	mov    $0x16,%ebp
   49d65:	jmp    49c10 <btree_iterator_next+0x1c0>
   49d6a:	mov    0x8(%rbx),%rdi
   49d6e:	mov    %rsi,0x8(%rsp)
   49d73:	call   35090 <btree_read_leaf_blob.lto_priv.0>
   49d78:	mov    %eax,%ebp
   49d7a:	test   %eax,%eax
   49d7c:	jne    49c10 <btree_iterator_next+0x1c0>
   49d82:	mov    0x80(%rbx),%rax
   49d89:	mov    0x8(%rsp),%rsi
   49d8e:	movzwl 0x26(%rax),%edi
   49d92:	jmp    49beb <btree_iterator_next+0x19b>

Disassembly of section .fini:
