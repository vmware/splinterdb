
.cache/invalid-address-perf-20261017/builds/sequential/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000049d50 <btree_iterator_next>:
   49d50:	push   %rbp
   49d51:	push   %rbx
   49d52:	sub    $0x18,%rsp
   49d56:	mov    0xb4(%rdi),%ebp
   49d5c:	test   %ebp,%ebp
   49d5e:	je     49d70 <btree_iterator_next+0x20>
   49d60:	add    $0x18,%rsp
   49d64:	mov    %ebp,%eax
   49d66:	pop    %rbx
   49d67:	pop    %rbp
   49d68:	ret
   49d69:	nopl   0x0(%rax)
   49d70:	mov    0xd8(%rdi),%esi
   49d76:	mov    %rdi,%rbx
   49d79:	test   %esi,%esi
   49d7b:	je     49d98 <btree_iterator_next+0x48>
   49d7d:	mov    0xcc(%rdi),%ecx
   49d83:	test   %ecx,%ecx
   49d85:	jne    49f30 <btree_iterator_next+0x1e0>
   49d8b:	mov    $0x1,%esi
   49d90:	mov    %rbx,%rdi
   49d93:	call   55310 <btree_iterator_maybe_start_prefetch.part.0.lto_priv.0>
   49d98:	mov    0x80(%rbx),%rdx
   49d9f:	mov    0x90(%rbx),%rax
   49da6:	movzwl 0x24(%rdx),%edx
   49daa:	add    $0x1,%rax
   49dae:	mov    %rax,0x90(%rbx)
   49db5:	cmp    %rdx,%rax
   49db8:	jne    49df0 <btree_iterator_next+0xa0>
   49dba:	mov    0xb4(%rbx),%edx
   49dc0:	test   %edx,%edx
   49dc2:	jne    49df0 <btree_iterator_next+0xa0>
   49dc4:	mov    0xa0(%rbx),%rcx
   49dcb:	cmp    %rcx,0x68(%rbx)
   49dcf:	je     49ff0 <btree_iterator_next+0x2a0>
   49dd5:	mov    %rbx,%rdi
   49dd8:	call   496e0 <btree_iterator_next_leaf>
   49ddd:	test   %eax,%eax
   49ddf:	jne    4a050 <btree_iterator_next+0x300>
   49de5:	data16 cs nopw 0x0(%rax,%rax,1)
   49df0:	movq   $0x0,0xb8(%rbx)
   49dfb:	cmpl   $0x2,0x1c(%rbx)
   49dff:	jne    49d60 <btree_iterator_next+0x10>
   49e05:	mov    0xb4(%rbx),%eax
   49e0b:	test   %eax,%eax
   49e0d:	jne    49d60 <btree_iterator_next+0x10>
   49e13:	mov    0x90(%rbx),%rdx
   49e1a:	mov    0xa0(%rbx),%rax
   49e21:	cmp    %rax,0x68(%rbx)
   49e25:	je     4a018 <btree_iterator_next+0x2c8>
   49e2b:	cmp    %rdx,0x98(%rbx)
   49e32:	jg     49fe8 <btree_iterator_next+0x298>
   49e38:	mov    0x80(%rbx),%rax
   49e3f:	mov    0x10(%rbx),%rsi
   49e43:	cmpb   $0x0,0x20(%rax)
   49e47:	movzwl 0x24(%rax),%ecx
   49e4b:	jne    49f70 <btree_iterator_next+0x220>
   49e51:	cmp    %cx,%dx
   49e54:	jae    4a060 <btree_iterator_next+0x310>
   49e5a:	movzwl %dx,%edx
   49e5d:	movzwl 0x28(%rax,%rdx,2),%ecx
   49e62:	cmp    0x22(%rax),%cx
   49e66:	jb     4a060 <btree_iterator_next+0x310>
   49e6c:	mov    0x10(%rsi),%rdx
   49e70:	movzwl 0x26(%rax),%r8d
   49e75:	lea    0x6(%rcx),%rsi
   49e79:	mov    %rdx,%r9
   49e7c:	mov    %r8,%rdi
   49e7f:	sub    %r8,%r9
   49e82:	cmp    %rsi,%r9
   49e85:	jb     4a060 <btree_iterator_next+0x310>
   49e8b:	lea    (%rax,%rcx,1),%rsi
   49e8f:	cmpb   $0x0,0x2(%rsi)
   49e93:	jne    4a060 <btree_iterator_next+0x310>
   49e99:	movzwl (%rsi),%eax
   49e9c:	mov    %rdx,%r10
   49e9f:	mov    $0x16,%ebp
   49ea4:	shr    $0x3,%r10
   49ea8:	lea    (%rax,%r8,1),%r9
   49eac:	cmp    %r9,%r10
   49eaf:	jb     49f10 <btree_iterator_next+0x1c0>
   49eb1:	movzbl 0x5(%rsi),%r9d
   49eb6:	test   $0x3,%r9b
   49eba:	je     4a060 <btree_iterator_next+0x310>
   49ec0:	cmp    $0x7,%r9b
   49ec4:	ja     4a060 <btree_iterator_next+0x310>
   49eca:	movzwl 0x3(%rsi),%r10d
   49ecf:	add    %rax,%r10
   49ed2:	lea    -0x6(%rdx),%rax
   49ed6:	sub    %r8,%rax
   49ed9:	sub    %rcx,%rax
   49edc:	cmp    %r10,%rax
   49edf:	jb     49f10 <btree_iterator_next+0x1c0>
   49ee1:	and    $0x4,%r9d
   49ee5:	jne    4a06a <btree_iterator_next+0x31a>
   49eeb:	mov    %rsi,0xb8(%rbx)
   49ef2:	test   %di,%di
   49ef5:	je     49fe8 <btree_iterator_next+0x298>
   49efb:	mov    %rbx,%rdi
   49efe:	call   44ed0 <btree_iterator_decode_current>
   49f03:	mov    %eax,%ebp
   49f05:	test   %eax,%eax
   49f07:	je     49fe8 <btree_iterator_next+0x298>
   49f0d:	nopl   (%rax)
   49f10:	movq   $0x0,0xb8(%rbx)
   49f1b:	mov    %ebp,0xb4(%rbx)
   49f21:	jmp    49d60 <btree_iterator_next+0x10>
   49f26:	cs nopw 0x0(%rax,%rax,1)
   49f30:	cmpl   $0x1,0xd4(%rdi)
   49f37:	je     49d98 <btree_iterator_next+0x48>
   49f3d:	lea    0xf8(%rdi),%rdi
   49f44:	call   ea30 <mini_meta_cursor_deinit@plt>
   49f49:	movl   $0x0,0xc8(%rbx)
   49f53:	movq   $0x0,0xcc(%rbx)
   49f5e:	movq   $0x0,0xf0(%rbx)
   49f69:	jmp    49d8b <btree_iterator_next+0x3b>
   49f6e:	xchg   %ax,%ax
   49f70:	cmp    %cx,%dx
   49f73:	jae    4a060 <btree_iterator_next+0x310>
   49f79:	movzwl %dx,%edx
   49f7c:	movzwl 0x28(%rax,%rdx,2),%edx
   49f81:	cmp    0x22(%rax),%dx
   49f85:	jb     4a060 <btree_iterator_next+0x310>
   49f8b:	movzwl 0x26(%rax),%ecx
   49f8f:	mov    0x10(%rsi),%rsi
   49f93:	sub    %rcx,%rsi
   49f96:	lea    0x17(%rdx),%rcx
   49f9a:	cmp    %rcx,%rsi
   49f9d:	jb     4a060 <btree_iterator_next+0x310>
   49fa3:	add    %rdx,%rax
   49fa6:	movzbl 0x16(%rax),%edi
   49faa:	cmp    $0x1,%dil
   49fae:	ja     4a060 <btree_iterator_next+0x310>
   49fb4:	movzwl 0x14(%rax),%ecx
   49fb8:	mov    %rcx,%r8
   49fbb:	add    $0x17,%rcx
   49fbf:	cmp    $0xfffd,%r8w
   49fc4:	mov    $0x17,%r8d
   49fca:	cmova  %r8,%rcx
   49fce:	test   %dil,%dil
   49fd1:	lea    0xc(%rcx),%r8
   49fd5:	cmovne %r8,%rcx
   49fd9:	sub    %rdx,%rsi
   49fdc:	cmp    %rcx,%rsi
   49fdf:	jb     4a060 <btree_iterator_next+0x310>
   49fe1:	mov    %rax,0xb8(%rbx)
   49fe8:	xor    %ebp,%ebp
   49fea:	jmp    49d60 <btree_iterator_next+0x10>
   49fef:	nop
   49ff0:	mov    0xa8(%rbx),%rdx
   49ff7:	cmp    %rdx,%rax
   49ffa:	jge    49df0 <btree_iterator_next+0xa0>
   4a000:	cmp    0x98(%rbx),%rdx
   4a007:	jne    49dd5 <btree_iterator_next+0x85>
   4a00d:	jmp    49df0 <btree_iterator_next+0xa0>
   4a012:	nopw   0x0(%rax,%rax,1)
   4a018:	mov    0xa8(%rbx),%rax
   4a01f:	xor    %ebp,%ebp
   4a021:	cmp    %rdx,%rax
   4a024:	jle    49d60 <btree_iterator_next+0x10>
   4a02a:	mov    0x98(%rbx),%rcx
   4a031:	cmp    %rcx,%rax
   4a034:	je     49d60 <btree_iterator_next+0x10>
   4a03a:	cmp    %rdx,%rcx
   4a03d:	jg     49d60 <btree_iterator_next+0x10>
   4a043:	jmp    49e38 <btree_iterator_next+0xe8>
   4a048:	nopl   0x0(%rax,%rax,1)
   4a050:	mov    %eax,%ebp
   4a052:	jmp    49d60 <btree_iterator_next+0x10>
   4a057:	nopw   0x0(%rax,%rax,1)
   4a060:	mov    $0x16,%ebp
   4a065:	jmp    49f10 <btree_iterator_next+0x1c0>
   4a06a:	mov    0x8(%rbx),%rdi
   4a06e:	mov    %rsi,0x8(%rsp)
   4a073:	call   35390 <btree_read_leaf_blob.lto_priv.0>
   4a078:	mov    %eax,%ebp
   4a07a:	test   %eax,%eax
   4a07c:	jne    49f10 <btree_iterator_next+0x1c0>
   4a082:	mov    0x80(%rbx),%rax
   4a089:	mov    0x8(%rsp),%rsi
   4a08e:	movzwl 0x26(%rax),%edi
   4a092:	jmp    49eeb <btree_iterator_next+0x19b>

Disassembly of section .fini:
